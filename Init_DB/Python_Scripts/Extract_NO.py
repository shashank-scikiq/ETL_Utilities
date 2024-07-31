import asyncpg
from utils import source_params
from utils import timing_decorator
import log_config
from datetime import datetime
import os
import asyncio
import env_defs as ed
import sys
from dotenv import load_dotenv
import utils
import pandas as pd


semaphore = asyncio.Semaphore(10)  # For Postgresql operation.
src_schema = os.getenv("SRC_SCHEMA_NAME")
src_no_table = os.getenv("SRC_NO_TBL_NAME")
tgt_schema = os.getenv("POSTGRES_SCHEMA")
tgt_table = os.getenv("SELLER_TBL")
src_logger = log_config.start_log()


# Extract the data from Providers' Table (NO)
# ======================================================================================

async def fetch_date_range():
    try:
        src_logger.info("Reading the Valid Date Ranges")
        conn = await asyncpg.connect(**source_params)
        query = f"""SELECT DISTINCT "Date" AS src_date
            FROM {src_schema}.{src_no_table}
            WHERE "Date" > Date'{os.getenv('START_DATE')}';"""

        records = await conn.fetch(query)
        await conn.close()
        return [record['src_date'] for record in records]

    except asyncpg.PostgresError as e:
        src_logger.error("Error fetching data: {e}")
        sys.exit()


async def dump_data_for_day(date: datetime.date):
    cols = ["provider_key", "order_date", "category", "sub_category", "Pincode"]
    tmp_df = pd.DataFrame(columns=cols)
    query = f"""select concat("Seller App", concat('__',provider_id)) as provider_key,
            "Date" as order_date, 
            trim(category) as category, 
            trim("Sub - Category") as sub_category,
            pin_code as "Pincode"
            from {src_schema}.{src_no_table} 
            where "Date" = $1"""
    try:
        src_logger.info(f"Getting Data for {date}")
        async with semaphore:
            conn = await asyncpg.connect(**source_params)
            records = await conn.fetch(query, date)
    except Exception as e:
        src_logger.error(f"{datetime.now()}: Error !!! {e.args[0]}")
        raise asyncpg.PostgresError
    else:
        src_logger.info(f"Successfully got data for {date}")
        df1 = pd.DataFrame(records, columns=cols)
        tmp_df = pd.concat([tmp_df, df1], ignore_index=True)

        tmp_df.to_parquet(ed.total_orders + f"{datetime.strftime(date, format='%Y-%m-%d')}_sku_rc.parquet")

        del (df1, tmp_df)
    finally:
        src_logger.info(f"Closing the connection for {date}.")
        if conn:
            await conn.close()


@timing_decorator
async def query_no_tables():
    src_logger.info("Extracting the Data right now.")
    src_logger.info(f" Starting to get data from Source NO Tables.")
    day_range = await fetch_date_range()
    tasks = [dump_data_for_day(day) for day in day_range]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        raise e
    else:
        src_logger.info("Process Completed.")
        
if __name__ == "__main__":
    if utils.chk_req_envs(ed.req_envs):
        src_logger.info("Environment Variables Loaded.")
    elif load_dotenv(ed.env_file):
        src_logger.info("Loading Env manually.")
    else:
        src_logger.error("Error loading environment files.")
        src_logger.error("Exiting.")
        sys.exit()

    try:
        os.path.exists(ed.dump_loc)
    except Exception as e:
        print(f"{ed.dump_loc} not found.")
        raise e
    else:
        print(f"Will Dump data to {ed.dump_loc}.")
        print(ed.dump_loc)
        print(ed.raw_files)
        print(ed.processed_files)
        print(ed.total_orders)
        print(os.getenv('DATA_DUMP_LOC'))
        asyncio.run(query_no_tables())
        