import asyncio
import asyncpg
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
from pyarrow import csv
from dotenv import load_dotenv
import os
import sys
import log_config
from utils import timing_decorator
import env_defs as ed

# parse_options = csv.ParseOptions(newlines_in_values = True)

source_params = {
    'host': f"{os.getenv('SRC_POSTGRES_HOST')}",
    'port': f"{os.getenv('SRC_POSTGRES_PORT')}",
    'database': f"{os.getenv('SRC_POSTGRES_DB')}",
    'user': f"{os.getenv('SRC_POSTGRES_USER')}",
    'password': f"{os.getenv('SRC_POSTGRES_PWD')}",
    'timeout': 60,
}

tgt_params = {
    'host': f"{os.getenv('POSTGRES_HOST')}",
    'port': int(f"{os.getenv('POSTGRES_PORT')}"),
    'database': f"{os.getenv('POSTGRES_DB')}",
    'user': f"{os.getenv('POSTGRES_USER')}",
    'password': f"{os.getenv('POSTGRES_PASSWORD')}",
    'timeout': 60
}

src_schema = os.getenv("SRC_SCHEMA_NAME")
src_no_table = os.getenv("SRC_NO_TBL_NAME")

src_logger = log_config.start_log()

tgt_schema = os.getenv("POSTGRES_SCHEMA")
tgt_table = os.getenv("SELLER_TBL")
mp = os.getenv("MONTHLY_PROVIDERS_TBL")

# Semaphore to limit concurrent connections
semaphore = asyncio.Semaphore(10)


async def fetch_date_range():
    try:
        src_logger.info("Reading the Valid Date Ranges")
        conn = await asyncpg.connect(**source_params)
        query = f"""SELECT DISTINCT "Date" AS src_date
            FROM {src_schema}.{src_no_table}
            WHERE "Date" > '2023-10-31';"""

        records = await conn.fetch(query)
        await conn.close()
        return [record['src_date'] for record in records]

    except asyncpg.PostgresError as e:
        src_logger.error("Error fetching data: {e}")
        sys.exit()
    # finally:
    #     await conn.close()


async def dump_data_for_day(date: datetime.date):
    cols = ["provider_key", "order_date", "category", "sub_category", "Pincode"]
    tmp_df = pd.DataFrame(columns=cols)
    query = f"""select concat(trim("Seller App"), concat('__',trim(provider_id))) as provider_key,
            "Date" as order_date, category, "Sub - Category" as sub_category,
            pin_code as "Pincode"
            from {src_schema}.{src_no_table} 
            where "Date" = $1"""
    try:
        src_logger.info(f"Getting Data for {date}")
        async with semaphore:
            conn = await asyncpg.connect(**source_params)
            records = await conn.fetch(query, date)
    except Exception as e:
        print(f"{datetime.now()}: Error !!! {e}")
        raise asyncpg.PostgresError
    else:
        src_logger.info(f"Successfully got data for {date}")
        df1 = pd.DataFrame(records, columns=cols)
        tmp_df = pd.concat([tmp_df, df1], ignore_index=True)
        
        tmp_df.to_parquet(f"ed.dump_loc{datetime.strftime(date,format='%Y-%m-%d')}_sku_rc.parquet")
        
        del (df1, tmp_df)
    finally:
        src_logger.info(f"Closing the connection.")
        if conn:
            await conn.close()


async def create_table():
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {tgt_schema}.{tgt_table}(
    provider_key text NULL,
    order_date date NULL,
    category varchar(200) NULL,
    sub_category varchar(100) NULL,
    seller_district varchar(100) NULL,
    seller_state varchar(100) NULL,
    seller_state_code varchar(200) NULL);"""

    conn = await asyncpg.connect(**tgt_params)
    try:
        await conn.execute(create_table_query)
        print(f"{datetime.now()}: Table '{tgt_table}' checked/created.")
    except Exception as e:
        print(f"{datetime.now()}: Error creating table '{tgt_table}': {e}")
        sys.exit()
    finally:
        await conn.close()


async def exec_qry(query: str):
    conn = await asyncpg.connect(**tgt_params)
    try:
        await conn.execute(query)
        print(f"{datetime.now()}: Successfully Executed query.")
    except Exception as e:
        print(f"{datetime.now()}: Failed to execute query with exception : {e}")
        sys.exit()
    finally:
        await conn.close()


async def enrich_export(parquet_file: str):
    try:
        async with semaphore:
            conn = await asyncpg.connect(**tgt_params)
            table = pq.read_table(parquet_file)

            pincode_df = pd.read_parquet(f"{ed.script_loc}pc.parquet")
            pincode_df["Pincode"] = pincode_df["Pincode"].astype(str)
            pincode_table = pa.Table.from_pandas(df=pincode_df)
            table = table.join(pincode_table,
                               keys='Pincode')

            table = table.drop("Pincode")
            table = table.rename_columns([
                "provider_key", "order_date", "category", "sub_category",
                "seller_district", "seller_state", "seller_state_code"])

            target_schema = pa.schema([
                ('provider_key', pa.string()),
                ('order_date', pa.date32()),
                ('category', pa.string()),
                ('sub_category', pa.string()),
                ('seller_district', pa.string()),
                ('seller_state', pa.string()),
                ('seller_state_code', pa.string())
            ])
            table = table.cast(target_schema)
            csv_buffer = pa.BufferOutputStream()
            pcsv.write_csv(table, csv_buffer)
            csv_data = csv_buffer.getvalue().to_pybytes()

            async with conn.transaction():
                await conn.copy_records_to_table(table_name=tgt_table, schema_name=tgt_schema,
                                                 records=pa.csv.read_csv(pa.BufferReader(csv_data)).to_pandas().values)

            print(f"{datetime.now()}: Loaded {parquet_file} into Postgresql table {tgt_table}")

    except Exception as e:
        print(f"{datetime.now()}: Error loading {parquet_file} into Postgresql: {e}")

    finally:
        if conn:
            await conn.close()
            print(f"{datetime.now()}: Closed connection to Postgresql")


@timing_decorator
async def get_sellers():
    src_logger.info("*********************************************************")
    src_logger.info(" Writing the Data to the target table. ")

    src_logger.info("Creating / Checking the target table.")
    await create_table()

    src_logger.info("Truncating the target table.")
    sql_qry = f"""TRUNCATE TABLE {tgt_schema}.{tgt_table} CONTINUE IDENTITY RESTRICT;"""
    await exec_qry(sql_qry)

    start_time = datetime.now()
    parquet_files = [os.path.join(ed.total_orders, f) for f in os.listdir(ed.total_orders) if f.endswith('_sku_rc.parquet')]
    print(f"{datetime.now()}: Found {len(parquet_files)} Parquet files to process")

    tasks = [enrich_export(parquet_file) for parquet_file in parquet_files]
    await asyncio.gather(*tasks)

    print(f"{datetime.now()}: Finished the import process in {datetime.now() - start_time}")

    src_logger.info("*********************************************************")

    src_logger.info("Creating the monthly providers' table.")
    sql_qry = f"""CREATE TABLE IF NOT EXISTS {tgt_schema}.{mp} (provider_key varchar(255) NULL,
    seller_district varchar(50) NULL, seller_state varchar(2000) NULL,
    seller_state_code varchar(50) NULL, order_month int4 NULL, order_year int4 NULL);"""

    await exec_qry(sql_qry)

    src_logger.info("Truncating the data in the Monthly Provider's table.")
    sql_qry = f"""TRUNCATE TABLE {tgt_schema}.{mp} CONTINUE IDENTITY RESTRICT;"""
    await exec_qry(sql_qry)

    sql_qry = f"""INSERT INTO {tgt_schema}.{mp}
        (provider_key, seller_district, seller_state, seller_state_code, order_month, order_year)
        select distinct provider_key as provider_key,
        seller_district, seller_state, seller_state_code,
        date_part('month', order_date) as order_month,
        date_part('year', order_date) as order_year
        from {tgt_schema}.{tgt_table} dp 
        group by provider_key, seller_district,
        seller_state, seller_state_code, order_month, order_year"""
    src_logger.info("Now generating data in the Monthly providers' table")
    await exec_qry(sql_qry)

    # src_logger.info("Post ops Cleanup. ")
    # await ad.del_ops(glob.glob(dump_loc+"*.parquet"))


# if __name__ == "__main__":
#     asyncio.run(get_sellers())
