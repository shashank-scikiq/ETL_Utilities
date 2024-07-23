from pyathena import connect
from dotenv import load_dotenv
from datetime import datetime
import os
import log_config
import env_defs as ed
from utils import timing_decorator, chk_req_envs
import asyncio
import sys

app_logger = log_config.start_log()

if chk_req_envs(ed.req_envs):
    app_logger.info("Environment Variables Loaded.")
elif load_dotenv(ed.env_file):
    app_logger.info("Loading Env manually.")
else:
    app_logger.error("Error loading environment files.")
    app_logger.error("Exiting.")
    sys.exit()

tbl_dt = {
    f"{os.getenv('ATH_TBL_B2C')}": """date(date_parse("O_Created Date & Time",'%Y-%m-%dT%H:%i:%s'))""",
    f"{os.getenv('ATH_TBL_B2B')}": """date(date_parse("O_Created Date & Time",'%Y-%m-%dT%H:%i:%s'))""",
    f"{os.getenv('ATH_TBL_VOUCHER')}": """date("o_created_at_date")""",
    f"{os.getenv('ATH_TBL_LOG')}": """date("order_created_at")"""
}


async def run_gen_qry(date_field_name: str = "", tbl_name: str = "", mnth=6, validate_data=True) -> str:
    try:
        conn_src_athena = connect(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=os.getenv('S3_STAGING_DIR'),
            region_name=os.getenv('AWS_REGION'),
            schema_name=os.getenv('SCHEMA_NAME')
        )
    except Exception as e:
        print(e.args[0])
        raise RuntimeError
    else:
        app_logger.info(f"Querying the table {tbl_name}.")

    if validate_data:
        stmt = f"""select count(1) as Row_Count 
                from {os.getenv('ATH_DB')}.{tbl_name} where extract (month from {date_field_name}) = {mnth}"""
        with conn_src_athena.cursor() as cur:
            try:
                out = cur.execute(stmt).fetchall()
            except Exception as e:
                app_logger.error(e.args[0])
            else:
                conn_src_athena.close()
                print(tbl_name, out)
                return tbl_name, out
    else:
        stmt = f"""select distinct {date_field_name} from {os.getenv("ATH_DB")}.{tbl_name} 
        where {date_field_name} >= date('{os.getenv("START_DATE")}') order by 1;"""

        with conn_src_athena.cursor() as cur:
            try:
                out = cur.execute(stmt).fetchall()
            except Exception as e:
                app_logger.error(e.args[0])
            else:
                conn_src_athena.close()
                return tbl_name, out


@timing_decorator
async def get_date_ranges():
    result_dict = {}
    dt_count = {}
    tasks = [run_gen_qry(dt_field, tbl_name, validate_data=False) for tbl_name, dt_field in tbl_dt.items()]
    result = await asyncio.gather(*tasks)

    for x, y in result:
        dt_count[x] = len(y)
        result_dict[x] = y

    print(dt_count)

    return result_dict


@timing_decorator
async def is_there_data_in_aws(mnth: str):
    result_dict = {}
    tasks = [run_gen_qry(dt_field, tbl_name, mnth=mnth, validate_data=True) for tbl_name, dt_field in tbl_dt.items()]
    result = await asyncio.gather(*tasks)
    for x, y in result:
        result_dict[x] = y[0]
    return result_dict

# async def main():
#     date_ranges = await get_date_ranges()
#     print(date_ranges.keys())

#     is_data = await run_tbl_chk(6)
#     print(is_data)

# if __name__ == "__main__":
#     asyncio.run(main())
