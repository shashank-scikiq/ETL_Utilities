import asyncpg
import asyncio
import os
from dotenv import load_dotenv
import utils
import env_defs as ed
from utils import timing_decorator, chk_req_envs
import log_config
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

L1_TBLS, L2_TBLS = utils.generate_req_envs(ed.req_envs, ed.environment)

user = os.getenv('POSTGRES_USER')
passwd = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
db = os.getenv('POSTGRES_DB')
schema_name = os.getenv("POSTGRES_SCHEMA")

pg_url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"


async def get_database_connection(source_params: str):
    return await asyncpg.connect(source_params)


@timing_decorator
async def run_script(script_file: str):
    db_conn = await get_database_connection(pg_url)
    # to_run = utils.read_clean_script(script_file, ed.env_file)
    to_run = utils.read_clean_script(script_file, ed.req_envs)
    try:
        await db_conn.execute(to_run)
        print(f"Script {script_file} executed successfully.")
    except asyncpg.PostgresError as e:
        print(f"Error executing script:-{script_file} {e}")
        raise e
    finally:
        await db_conn.close()


async def run_files(sql_files):
    tasks = [run_script(sql_file) for sql_file in sql_files]
    await asyncio.gather(*tasks)


async def business_logic():
    """
    Check if any table from seq1 is in L2 tables, then run it.
    Followed by any table from seq2 then L3 tables.
    """
    async def run_seq(seq):
        runner_files = []
        for tbl in seq:
            if tbl in L2_TBLS:
                print(f"Will run {tbl}.")
                print(ed.INS_TBL_MAPPING_SQL[tbl])
                runner_files.append(ed.INS_TBL_MAPPING_SQL[tbl])
        return runner_files
                
    print(L2_TBLS)
    
    sql_list_1 = await run_seq(ed.seq1)
    sql_list_2 = await run_seq(ed.seq2)
    sql_list_3 = await run_seq(ed.seq3)
    
    print("\nRunning First Level Sequence files.")
    await run_files(sql_list_1)

    print("\nRunning Second Level Sequence files.")
    await run_files(sql_list_2)

    print("\nRunning Third Level Sequence files.")
    await run_files(sql_list_3)
      

if __name__ == "__main__":
    asyncio.run(business_logic())
