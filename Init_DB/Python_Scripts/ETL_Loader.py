import os
import asyncio
import asyncpg
import pandas as pd
import log_config
from datetime import datetime, timedelta
from dotenv import load_dotenv
import utils
from utils import timing_decorator
import pytz
from email_app import send_message
import Load_DB
from populate_dim_providers import get_sellers
from CREATE_TBLS import table_ops
from sqlalchemy import create_engine
import env_defs as ed
from get_start_date_tables import get_date_ranges, is_there_data_in_aws
import sys
from Extract_SRC import query_no_tables, query_athena_db
from RUN_Business_Logic_async import business_logic
from Transform_Data import transform_data

# Initialize logging
app_logger = log_config.start_log()

# Constants and configurations
user = os.getenv('POSTGRES_USER')
passwd = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
db = os.getenv('POSTGRES_DB')
schema_name = os.getenv("POSTGRES_SCHEMA")

env_status = utils.chk_req_envs(ed.req_envs)
if env_status:
    app_logger.info("Environment Variables Loaded.")
elif load_dotenv(ed.env_file):
    app_logger.info("Loading Env manually.")
else:
    app_logger.error("Error loading environment files.")
    app_logger.error("Exiting.")
    sys.exit()

pg_url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
L1_TBLS, L2_TBLS = utils.generate_req_envs(f_name=ed.req_envs,
                                           env=ed.environment)


@timing_decorator
async def create_schema(db_conn):
    qry = f"""CREATE SCHEMA IF NOT EXISTS "{schema_name}" 
    AUTHORIZATION "{os.getenv('POSTGRES_USER')}";"""
    try:
        app_logger.info(f"Creating schema {schema_name}.")
        await db_conn.execute(qry)
    except asyncpg.PostgresError as e:
        app_logger.error(f"Error creating schema: {e}")
        return False
    return True


async def trunc_tbls(tbl_name, pool):
    qry = f"""TRUNCATE TABLE ONLY "{os.getenv('POSTGRES_SCHEMA')}"."{tbl_name}" 
    CONTINUE IDENTITY CASCADE"""

    async with pool.acquire() as db_conn:
        app_logger.debug(f"Now Truncating --> {tbl_name}")
        try:
            await db_conn.execute(qry)
            app_logger.debug(f"Truncated {tbl_name}")
        except asyncpg.PostgresError as e:
            app_logger.error(f"Error truncating {tbl_name}: {e}")


@timing_decorator
async def run_script(db_conn, script_file: str):
    to_run = utils.read_clean_script(script_file)
    try:
        await db_conn.execute(to_run)
        app_logger.info("Script executed successfully.")
    except asyncpg.PostgresError as e:
        app_logger.error(f"Error executing script: {e}")
        raise e


async def insert_into_pincode(db_url: str):
    engine = create_engine(db_url)
    df = pd.read_parquet(f"{ed.script_loc}pc.parquet")
    app_logger.info(df.head(2))
    try:
        df.to_sql(name=os.getenv("TBL_PINCODE"), con=engine, schema=f'{os.getenv("POSTGRES_SCHEMA")}',
                  if_exists='replace')
    except Exception as e:
        app_logger.error(e)
        return False
    else:
        app_logger.debug("Successfully Generated data in the Zipcode file.")
        return True
    finally:
        engine.dispose()


@timing_decorator
async def trunc_tbls_async(tbls, db_conn):
    tasks = [trunc_tbls(os.getenv(tbl), db_conn) for tbl in tbls]
    await asyncio.gather(*tasks)


async def create_pool(source_params):
    return await asyncpg.create_pool(source_params)


@timing_decorator
async def ETL_initialization():
    # TODO: 
    # 1) Add Data Quality. (Runs Separately.)
    # 2) Separate Extraction and Separate loading. (Done.)
    # 3) IF the data has been extracted, no extraction needed. <<IMP>>
    # 4) Loading functions can be called out separately. (Done.)

    email_log = []
    time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime(format="%H:%M:%S")
    email_log.append("Started ETL Process at {} (IST)".format(time))

    # Check if there is data in AWS Athena's tables.
    delta = datetime.now() - timedelta(weeks=4)
    out = await is_there_data_in_aws(delta.month)
    df = pd.DataFrame(out)
    for col in df.columns:
        if df[col][0] < 1:
            print(f"No Data in {col} table.")
            email_log.append(f"No Data in {col} table.")
            email_log.append(f"Time = {time}")
            print(email_log)
            send_message(email_log)
            sys.exit()

    # Check if Data dump exists. If not start the dump process.
    parquet_files = await utils.find_parquet_files(ed.dump_loc)
    if parquet_files:
        app_logger.info("Source Directory anf files exists. ")
        app_logger.info("Proceeding with data load.")
    else:
        app_logger.debug("\nSource files not found. Starting the extract process.")
        app_logger.info("Getting data from AWS Athena.")
        await query_athena_db()

        app_logger.info("Getting data from NO Tables.")
        await query_no_tables()

        # Showing the count of the data downloaded. 
        utils.catalogue_files_src(ed.raw_files)

    # Create a connection pool
    pool = await create_pool(pg_url)

    try:
        async with pool.acquire() as db_conn:
            await create_schema(db_conn)

            # Creating the Tables based on Environment
            await table_ops()

            # Insert data into pincode table
            await insert_into_pincode(pg_url)

            # Truncate tables asynchronously
            await trunc_tbls_async(L1_TBLS, pool)
            await asyncio.sleep(2)

            app_logger.info("Checking Existing Transformed")
            parquet_files = await utils.find_parquet_files(ed.processed_files)
            if parquet_files:
                app_logger.info("Source Directory transformed files exists. ")
                app_logger.info("Proceeding with data load.")
            else:
                app_logger.debug("\nSource files not found. Starting the extract process.")
                app_logger.info("Transforming the data.")

                # Transforming the data
                await transform_data()

                app_logger.info("\nCatalogue of Files before.")
                utils.catalogue_files_src(ed.raw_files)
                print("Catalogue Read")

                app_logger.info("\nCatalogue of Files after.")
                utils.catalogue_files_tgt(ed.processed_files)

            # Load the Data into the DB. 
            await Load_DB.populate_data()

            # Truncate the Business Logic Data. 
            print("\nTruncating tables with Business logic.")
            await trunc_tbls_async(L2_TBLS, pool)

            # Populate the total sellers' table. 
            print("\nPopulating Total Sellers.")
            await get_sellers()

        # Generating Data in pincode as it gets cleared above. 
        # :TODO: Will fix this later. 
        # This is needed to generate DIM Districts.
        await insert_into_pincode(pg_url)

        # Populate the Rest of the Business Logic. 
        print("Run rest of the business logic.")
        await business_logic()

        # Add Indexing.

        # Sending Email.
        end_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime(format="%H:%M:%S")
        email_log.append(f"\n ETL Log for {os.getenv('EMAIL_ENV')}")
        email_log.append(f"\n ETL Process Finished at {format(end_time)} (IST)")
        email_log.append(f"\n Read Errors: \n")
        print(email_log)
        send_message(email_log)

    except Exception as e:
        app_logger.error(f"An error occurred in the ETL process: {e}")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(ETL_initialization())
