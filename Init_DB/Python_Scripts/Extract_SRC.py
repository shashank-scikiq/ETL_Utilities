import aioboto3
import pandas as pd
import asyncio
import os
from dotenv import load_dotenv
from utils import timing_decorator
from get_start_date_tables import get_date_ranges
from botocore.exceptions import ClientError
import utils
import log_config

import sys
import env_defs as ed

import pyarrow as pa
import pyarrow.parquet as pq

src_logger = log_config.start_log()

S3_LOCATION = os.getenv('S3_STAGING_DIR')
region_name = os.getenv('AWS_REGION')
DATABASE = os.getenv('ATH_DB')

aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
aws_region = os.getenv('AWS_REGION')
mp = os.getenv("MONTHLY_PROVIDERS_TBL")
max_concurrent_queries = 5
output_location = ed.raw_files

# Extract the Data from AWS Athena.
# ======================================================================================

def chunk_date_ranges(date_range, chunk_size=10):
    for i in range(0, len(date_range), chunk_size):
        yield date_range[i:i + chunk_size]


async def execute_athena_query(tbl_name, date_val, query, semaphore, max_retries=5):
    if not aws_access_key_id or not aws_secret_access_key:
        raise Exception("AWS credentials are not set in environment variables.")

    async with semaphore:
        async with aioboto3.Session().client('athena', region_name=aws_region,
                                             aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key) as client:
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
                    while status in ('RUNNING', 'QUEUED'):
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
                            response = await client.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token)
                        else:
                            response = await client.get_query_results(QueryExecutionId=query_execution_id)

                        if not column_info:
                            column_info = response['ResultSet']['ResultSetMetadata']['ColumnInfo']

                        results.extend(response['ResultSet']['Rows'])
                        next_token = response.get('NextToken')
                        if not next_token:
                            break

                    return {'column_info': column_info, 'rows': results}

                except ClientError as e:
                    if e.response['Error']['Code'] == 'TooManyRequestsException':
                        retries += 1
                        wait_time = min(2 ** retries, 60)
                        src_logger.error(f"TooManyRequestsException encountered for {tbl_name}:{date_val}. Retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        src_logger.error(f"ClientError encountered for {tbl_name}:{date_val}. Error: {e}")
                        raise e


def results_to_parquet(results: dict, file_path: str):
    # print(f"Writing Results to Parquet.{file_path}")
    columns = [col['Label'] for col in results['column_info']]
    rows = [list(map(lambda field: field.get('VarCharValue', ''), row['Data'])) for row in results['rows'][1:]]
    table = pa.Table.from_arrays(list(zip(*rows)), names=columns)
    pq.write_table(table, file_path)


@timing_decorator
async def process_data_athena(tbl_name: str, date_range: list, read_script_file: str, semaphore):
    completed_read = utils.read_clean_script(ed.script_loc + "/" + read_script_file, ed.req_envs)

    async def process_date(date_val):
        # src_logger.info(f"Processing File {tbl_name}.")
        formatted_date = date_val.strftime('%Y-%m-%d')
        formatted_query = completed_read.format(date_val=formatted_date)
        filename = f"query_result_{formatted_date}_{tbl_name}.parquet"
        file_path = os.path.join(output_location, filename)

        result = await execute_athena_query(tbl_name, date_val, formatted_query, semaphore)

        try:
            results_to_parquet(results=result, file_path=file_path)
        except Exception as e:
            raise e

        src_logger.info(f"Result for {tbl_name}:{formatted_date} Done.")

        df = pd.read_parquet(file_path)
        src_logger.info(f"Number of rows returned: {df.shape[0]}")

    date_chunks = chunk_date_ranges(date_range, chunk_size=20)
    for chunk in date_chunks:
        tasks = [process_date(date_val[0]) for date_val in chunk]
        await asyncio.gather(*tasks)


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

    semaphore = asyncio.Semaphore(max_concurrent_queries)

    if src_tbl_name:
        tmp_tbl = tbl_key_val[src_tbl_name]
        tasks = [process_data_athena(tbl_name=tmp_tbl,
                                     date_range=date_ranges[tmp_tbl],
                                     read_script_file=src_tbl_dict[tmp_tbl],
                                     semaphore=semaphore)]
    else:
        tasks = [process_data_athena(tbl_name=key,
                                     date_range=date_ranges[key],
                                     read_script_file=value,
                                     semaphore=semaphore) for key, value in src_tbl_dict.items()]

    await asyncio.gather(*tasks)


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
        asyncio.run(query_athena_db())
