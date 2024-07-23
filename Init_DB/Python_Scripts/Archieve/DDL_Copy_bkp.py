import sys
import psycopg
import log_config
from dotenv import load_dotenv
import os
import log_config
from pyathena import connect
from datetime import datetime, timedelta
import utils

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

import get_start_date_tables as get_sd

'''
1) Check if the Connection to the Athena Database is successful. 
2) Check if the connection to the Postgresql Data is successful. 
3) Check if the Postcode table exists in the Postgresql and has data in it. 
4) Update-load the data in the Postgresql Database. 
5) Update-load the data in the Postgres Views.
6) Report the outcome in the logs
'''
ddl_copy_logger = log_config.start_log()

if os.path.exists("/app/init_db/Final_DB_Scripts"):
    script_loc = "/app/init_db/Final_DB_Scripts"
    main_loc = "/app/init_db"
    load_dotenv("../../aws_common.env")
else:
    main_loc = "/home/sraj/Documents/GitHub/OD_DQ/Init_DB/"
    script_loc = main_loc+"Final_DB_Scripts"
    load_dotenv("/home/sraj/Documents/GitHub/OD_DQ/aws_common.env")
        


def check_date(dt_val):
    """
Enter date in YYYY-MM-DD Format.
Checks if a date is valid.
"""
    try:
        dt = datetime.strptime(dt_val, "%Y-%m-%d").date()
        # print(dt)
    except:
        ddl_copy_logger.error(
            "Use YYYY-MM-DD format with the right date value")
        return
    else:
        return dt


def get_postgres_connection():
    conn_params = {
        "dbname": os.getenv('POSTGRES_DB'),
        "user": os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "host": os.getenv('POSTGRES_HOST')
    }
    # ddl_copy_logger.info(conn_params)
    try:
        connection = psycopg.connect(**conn_params)
    except Exception as db_exc:
        ddl_copy_logger.error(f'Unable to connect to the DB with user {os.getenv("POSTGRES_USER")} on '
                              f'{os.getenv("POSTGRES_HOST")}')
        print(db_exc.args[0])
        raise ConnectionError
    else:
        ddl_copy_logger.info("Connected")
        return connection


def copy_to_Dest(f_src_qry, tgt_qry, date_range):
    # Take the source connection string.
    # Take the target connection string.
    # Take the start and end date.
    # Copy one day and paste it into the Database.
    # If Any day fails to read, exit the whole thing. 

    result: list = []
    # Connect to the Source Database.
    # ================================================================
    try:
        ddl_copy_logger.info("Trying to connect to AWS Athena")
        conn_src_athena = connect(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=os.getenv('S3_STAGING_DIR'),
            region_name=os.getenv('AWS_REGION'),
            schema_name=os.getenv('SCHEMA_NAME')
        )
        ddl_copy_logger.info(conn_src_athena)
    except:
        ddl_copy_logger.error("Unable to Connect to the Source Database")
        ddl_copy_logger.error(os.getenv("AWS_ACCESS_KEY"))
        sys.exit()
    else:
        ddl_copy_logger.info("Connected to Athena Database")

    # Connect to the target Database.
    # ================================================================

    try:
        ddl_copy_logger.info("Trying to connect to the target database.")
        conn_tgt_pg = get_postgres_connection()
    except:
        ddl_copy_logger.error("Unable to the target Database")
        sys.exit()
    else:
        ddl_copy_logger.info("Connected to target Postgres Database")
        ddl_copy_logger.info("Target creds are {}".format(conn_tgt_pg))

    failures = 0
    ddl_copy_logger.info("From {} to {}".format(date_range[0], date_range[len(date_range)-1]))

    # For Each 5 days (Use Multiprocessing)
    # ================================================================

    for date in date_range:
        ttl_time_st = datetime.now()
        src_qry = f_src_qry
        ddl_copy_logger.info(f"Current date is {date}")
        to_run = src_qry.replace("DATE_PARAM", datetime.strftime(date[0], format="%Y-%m-%d"))

        # Get data from the Source Database
        # ================================================================
        try:
            q_st_t = datetime.now()
            result = conn_src_athena.cursor().execute(to_run).fetchall()
            q_en_t = datetime.now()
        except Exception as e:
            ddl_copy_logger.error(f"Unable to read data for --> {date}")
            ddl_copy_logger.error(f"{e}")
            sys.exit()
        else:
            ddl_copy_logger.info(f"Read {len(result)} lines for {date} in {q_en_t - q_st_t} secs.")
            # ddl_copy_logger.info(result[:3])

        target_qry = tgt_qry.replace("AGG_TBL_B2C", os.getenv("AGG_TBL_B2C"))
        # ddl_copy_logger.info("\n Target query is \n", target_qry)

        # Write all the data at one go to the target database
        # ================================================================
        try:
            q_st_t = datetime.now()
            conn_tgt_pg.cursor().executemany(target_qry, (result))
            q_en_t = datetime.now()
        except Exception as e:
            ddl_copy_logger.error(
                "Unable to write to database for {}".format(date))
            ddl_copy_logger.error(str(e))
            # ddl_copy_logger.error({target_qry})
            conn_tgt_pg.rollback()
            sys.exit()
        else:
            ddl_copy_logger.info(f"Writing to Target Database for {date} done in {q_en_t - q_st_t} secs")
            conn_tgt_pg.commit()
    ttl_time_end = datetime.now()
    conn_src_athena.close()
    conn_tgt_pg.close()
    ddl_copy_logger.info(f"Total time taken {ttl_time_end - ttl_time_st} secs")
    return True


def run_agg_qry(read_script: str, write_scr: str, tbl_name: str, date_range: str) -> list:
    completed_read = utils.read_clean_script(script_loc + "//" + read_script)
    completed_write = utils.read_clean_script(script_loc + "//" + write_scr)

    print("Source date is ", date_range[0])
    
    ddl_copy_logger.debug(f"\n ********** Loading Data into {tbl_name}. ********** ")
    
    run_status = copy_to_Dest(completed_read, completed_write, date_range)
    
    return run_status
