from utils import timing_decorator
import os
import log_config
import aiofiles
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import MetaData, Table, Column, PrimaryKeyConstraint
from sqlalchemy.types import String, Text, Integer, Date, Numeric, Float, TIMESTAMP
from typing import Optional
from dotenv import load_dotenv
import asyncio
import utils
import asyncpg
import env_defs as ed
import sys


app_logger = log_config.start_log()

# Load environment variables
if utils.chk_req_envs(ed.req_envs):
    app_logger.info("Environment Variables Loaded.")
elif load_dotenv(ed.env_file):
    app_logger.info("Loading Env manually.")
else:
    app_logger.error("Error loading environment files.")
    app_logger.error("Exiting.")
    sys.exit()
    
    
# Constants and configurations
user = os.getenv('POSTGRES_USER')
passwd = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
db = os.getenv('POSTGRES_DB')
schema_name = os.getenv("POSTGRES_SCHEMA")
environment = os.getenv('DEPLOYMENT_TYPE')

asyncpg_url = f"postgresql+asyncpg://{user}:{passwd}@{host}:{port}/{db}"
L1_TBLS, L2_TBLS = utils.generate_req_envs(f_name=ed.req_envs, 
                                           env=environment)


# Function to establish a database connection
async def get_database_connection(source_params: str):
    return await asyncpg.connect(source_params)


@timing_decorator
async def read_table_struct_async(tbl_struct_file):
    dict_tbls = {}
    if os.path.exists(tbl_struct_file):
        app_logger.info(f"Found file: {tbl_struct_file}")
        try:
            async with aiofiles.open(tbl_struct_file, mode="r") as f:
                data = await f.readlines()
        except Exception as e:
            raise e
        key = ""
        vals = {}
        # print(data)
        for line in data:
            if line.startswith(">>>>"):
                vals = {}
                key = os.getenv(line.split(">>>>")[1].strip())
                dict_tbls[key] = ""
            elif ":" in line:
                k1, v1 = line.split(":")[0], line.split(":")[1].strip()
                vals[k1] = v1
                dict_tbls[key] = vals
        return dict_tbls
    else:
        app_logger.error(f"File not found: {tbl_struct_file}")
        raise FileNotFoundError(f"File not found: {tbl_struct_file}")


@timing_decorator
async def create_tables_concurrently(db_url, schema_name, table_definitions):
    app_logger.info("Creating tables concurrently.")
    engine = create_async_engine(db_url, echo=False)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        metadata = MetaData()
        for table_name, columns in table_definitions.items():
            columns_list = []
            for name, column_spec in columns.items():
                column_spec = eval(column_spec)
                data_type, length = column_spec[0], column_spec[1] if len(column_spec) > 1 else None
                column_type = get_column_type(data_type, length)
                column = Column(name, column_type)
                columns_list.append(column)
            table = Table(table_name, metadata, schema=schema_name, extend_existing=True)
            for column in columns_list:
                table.append_column(column)
            if 'id' in columns:
                table.append_constraint(PrimaryKeyConstraint('id', name=f"{table_name}_pkey"))
            app_logger.info(f"Processed Table {table_name}")
        async with engine.begin() as conn_obj:
            await conn_obj.run_sync(metadata.create_all)
        app_logger.info("Tables created successfully.")
    await engine.dispose()
    

def get_column_type(data_type: str, length: Optional[int]) -> type:
    type_mapping = {
        'varchar': String,
        'text': Text,
        'int4': Integer,
        'date': Date,
        'int8': Integer,
        'numeric': Numeric,
        'float': Float,
        'timestamp': TIMESTAMP
    }
    if length:
        return type_mapping.get(data_type.lower())(length)
    else:
        return type_mapping.get(data_type.lower())
    
    
async def table_ops():
    # tbl_struct_file = os.path.join(ed.script_loc, ed.tbl_names)
    # print(tbl_struct_file)
    table_definitions = await read_table_struct_async(ed.tbl_names)
    
    # Change based on Environment. Different tables will go into Prod and Staging Environment.
    filtered_tables = [os.getenv(tbl) for tbl in L1_TBLS]
    [filtered_tables.append(os.getenv(tbl)) for tbl in L2_TBLS]
    
    final_def = {}
    for tbl, structure in table_definitions.items():
        if tbl in filtered_tables:
            final_def[tbl] = structure
    
    if len(filtered_tables) == len(final_def.keys()):
        print("Filtered tables will be sent for creation.")
    
    # Creating Tables Asynchronously.
    await create_tables_concurrently(asyncpg_url, schema_name, final_def)


if __name__ == "__main__":
    print("Running Main")
    asyncio.run(table_ops())
    