import aioboto3
import pandas as pd
import asyncio
import os
from dotenv import load_dotenv
from datetime import datetime
from utils import timing_decorator
from get_start_date_tables import get_date_ranges
from botocore.exceptions import ClientError
import utils
import log_config
import asyncpg
from utils import source_params
import sys
import env_defs as ed

import pyarrow as pa
import pyarrow.parquet as pq

src_logger = log_config.start_log()
src_schema = os.getenv("SRC_SCHEMA_NAME")
src_no_table = os.getenv("SRC_NO_TBL_NAME")

tgt_schema = os.getenv("POSTGRES_SCHEMA")
tgt_table = os.getenv("SELLER_TBL")
S3_LOCATION = os.getenv('S3_STAGING_DIR')
region_name = os.getenv('AWS_REGION')
DATABASE = os.getenv('ATH_DB')

aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
aws_region = os.getenv('AWS_REGION')
mp = os.getenv("MONTHLY_PROVIDERS_TBL")
sem = 2  # Adjust the concurrency limit to a lower value
semaphore = asyncio.Semaphore(10)  # For Postgresql operation.


# Extract the Data from AWS Athena. 
# ======================================================================================

def chunk_date_ranges(date_range, chunk_size=10):
    for i in range(0, len(date_range), chunk_size):
        yield date_range[i:i + chunk_size]


# async def execute_athena_query(tbl_name, date_val, semaphore, client, database, query, output_location, max_retries=5):
#     async with semaphore:
#         retries = 0
#         while retries < max_retries:
#             try:
#                 start_time = datetime.now()
#                 response = await client.start_query_execution(
#                     QueryString=query,
#                     QueryExecutionContext={'Database': database},
#                     ResultConfiguration={'OutputLocation': output_location}
#                 )
#                 query_execution_id = response['QueryExecutionId']
#
#                 while True:
#                     response = await client.get_query_execution(QueryExecutionId=query_execution_id)
#                     status = response['QueryExecution']['Status']['State']
#
#                     if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
#                         break
#
#                     await asyncio.sleep(1)
#
#                 end_time = datetime.now()
#                 duration = (end_time - start_time).total_seconds()
#
#                 if status == 'SUCCEEDED':
#                     results = []
#                     next_token = None
#
#                     while True:
#                         if next_token:
#                             response = await client.get_query_results(
#                                 QueryExecutionId=query_execution_id,
#                                 NextToken=next_token
#                             )
#                         else:
#                             response = await client.get_query_results(QueryExecutionId=query_execution_id)
#
#                         columns = [col['Name'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
#                         rows = response['ResultSet']['Rows'][1:]
#
#                         for row in rows:
#                             data = [item.get('VarCharValue', None) for item in row['Data']]
#                             results.append(data)
#
#                         next_token = response.get('NextToken')
#                         if not next_token:
#                             break
#
#                     df = pd.DataFrame(results, columns=columns)
#                     row_count = len(df)
#
#                     return df, duration, row_count
#                 else:
#                     state_change_reason = response['QueryExecution']['Status'].get('StateChangeReason',
#                                                                                    'No reason provided')
#                     src_logger.error(f"****** {tbl_name} : {date_val} ******")
#                     raise Exception(f"Query failed with status: {status}. Reason: {state_change_reason}")
#
#             except ClientError as e:
#                 if e.response['Error']['Code'] == 'TooManyRequestsException':
#                     retries += 1
#                     wait_time = min(2 ** retries, 60)  # Exponential backoff
#                     src_logger.error(f"TooManyRequestsException encountered. Retrying in {wait_time} seconds...")
#                     await asyncio.sleep(wait_time)
#                 else:
#                     raise e


# @timing_decorator
# async def query_athena(tbl_name, date_val, database, query, output_location, region_name=os.getenv('AWS_REGION'),
#                        max_concurrent_queries=sem):
#     aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
#     aws_secret_access_key = os.getenv('AWS_SECRET_KEY')

#     if not aws_access_key_id or not aws_secret_access_key:
#         raise Exception("AWS credentials are not set in environment variables.")

#     semaphore = asyncio.Semaphore(max_concurrent_queries)

#     async with aioboto3.Session().client(
#             'athena',
#             region_name=region_name,
#             aws_access_key_id=aws_access_key_id,
#             aws_secret_access_key=aws_secret_access_key
#     ) as client:
#         df, duration, row_count = await execute_athena_query(tbl_name, date_val, semaphore, client, database, query,
#                                                              output_location)
#         return df, duration, row_count

@timing_decorator
async def execute_athena_query(tbl_name, date_val, query, max_concurrent_queries=2, max_retries=5):
    if not aws_access_key_id or not aws_secret_access_key:
        raise Exception("AWS credentials are not set in environment variables.")
    
    semaphore = asyncio.Semaphore(max_concurrent_queries)

    async with aioboto3.Session().client('athena', region_name=aws_region, 
                                         aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key) as client:
        async with semaphore:
            retries = 0
            while retries < max_retries:
                try:
                    response = await client.start_query_execution(
                        QueryString=query,
                        QueryExecutionContext={'Database': DATABASE},
                        ResultConfiguration={'OutputLocation': S3_LOCATION}
                    )
                    query_execution_id = response['QueryExecutionId']

                    src_logger.info(f"Executing query for : {tbl_name}:{date_val}")
                    status = 'RUNNING'
                    while status in ('RUNNING','QUEUED'):
                        response = await client.get_query_execution(QueryExecutionId=query_execution_id)
                        status = response['QueryExecution']['Status']['State']

                        if status in ('FAILED', 'CANCELLED'):
                            state_change_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'No reason provided')
                            src_logger.error(f"Query failed for {tbl_name}:{date_val} with status: {status}. Reason: {state_change_reason}")
                            raise Exception(f"Query failed with status: {status}. Reason: {state_change_reason}")
                        await asyncio.sleep(2)

                    results = []
                    next_token = None
                    column_info = None

                    while True:
                        if next_token:
                            response = await client.get_query_results(QueryExecutionId=query_execution_id,NextToken=next_token)
                        else:
                            response = await client.get_query_results(QueryExecutionId=query_execution_id)

                        if not column_info:
                            column_info = response['ResultSet']['ResultSetMetadata']['ColumnInfo']

                        results.extend(response['ResultSet']['Rows'])
                        next_token = response.get('NextToken')
                        if not next_token:
                            break

                except ClientError as e:
                    if e.response['Error']['Code'] == 'TooManyRequestsException':
                        retries += 1
                        wait_time = min(2 ** retries, 60)
                        src_logger.error(f"TooManyRequestsException encountered for {tbl_name}:{date_val}. Retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        src_logger.error(f"ClientError encountered for {tbl_name}:{date_val}. Error: {e}")
                        raise e
            return {'column_info':column_info, 'rows':results}
        

def results_to_parquet(results: str, file_path: str):
    columns = [col['Label'] for col in results['column_info']]
    rows = [list(map(lambda field: field.get('VarCharValue', ''), row['Data'])) for row in results['rows'][1:]]
    table = pa.Table.from_arrays(list(zip(*rows)), names=columns)
    pq.write_table(table, file_path)


@timing_decorator
async def process_data_athena(tbl_name: str, date_range: list, read_script_file: str, output_location: str):
    completed_read = utils.read_clean_script(ed.script_loc + "/" + read_script_file, ed.req_envs)

    async def process_date(date_val):
        formatted_date = date_val.strftime('%Y-%m-%d')
        formatted_query = completed_read.format(date_val=formatted_date)
        filename = f"query_result_{formatted_date}_{tbl_name}.parquet"

        result = await execute_athena_query(tbl_name, date_val, formatted_query)
        results_to_parquet(results=result, file_path=filename)

        src_logger.info(f"Result for {tbl_name}:{formatted_date} Done.")

        # df.to_parquet(ed.raw_files + filename)
        # src_logger.info(f"Execution time: {duration} seconds")
        # src_logger.info(f"Number of rows returned: {row_count}")

    date_chunks = chunk_date_ranges(date_range, chunk_size=10)
    for chunk in date_chunks:
        tasks = [process_date(date_val[0]) for date_val in chunk]
        await asyncio.gather(*tasks)


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
async def query_athena_db(src_tbl_name: str = ""):
    """
    Will run the data dump from AWS Athena either for all tables or 
    the value specified in src_tbl_name.
    
    Valid names for src_tbl_name are ["ATH_TBL_B2C","ATH_TBL_B2B", 
    "ATH_TBL_VOUCHER", "ATH_TBL_LOG"]
    
    """

    tbl_key_val = {
        "ATH_TBL_B2C": os.getenv("ATH_TBL_B2C"),
        "ATH_TBL_B2B": os.getenv("ATH_TBL_B2B"),
        "ATH_TBL_VOUCHER": os.getenv("ATH_TBL_VOUCHER"),
        "ATH_TBL_LOG": os.getenv("ATH_TBL_LOG")
    }

    src_logger.info("Extracting the Data right now.")
    src_tbl_dict = {
        os.getenv("ATH_TBL_B2C"): "ATH_TBL_B2C.sql",
        os.getenv("ATH_TBL_B2B"): "ATH_TBL_B2B.sql",
        os.getenv("ATH_TBL_VOUCHER"): "ATH_TBL_VOUCHER.sql",
        os.getenv("ATH_TBL_LOG"): "ATH_TBL_LOG.sql",
    }

    src_logger.info("Fetching Date Ranges.")
    date_ranges = await get_date_ranges()
    src_logger.info("Date ranges obtained.")

    src_logger.info(src_tbl_dict)

    if src_tbl_name:
        tmp_tbl = tbl_key_val[src_tbl_name]
        tasks = [process_data_athena(tbl_name=tmp_tbl,
                           date_range=date_ranges[tmp_tbl],
                           read_script_file=src_tbl_dict[tmp_tbl])]
    else:
        tasks = [process_data_athena(tbl_name=key,
                           date_range=date_ranges[key],
                           read_script_file=value) for key, value in src_tbl_dict.items()]

    await asyncio.gather(*tasks)


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
        # asyncio.run(query_no_tables())
        asyncio.run(query_athena_db("ATH_TBL_B2C"))
