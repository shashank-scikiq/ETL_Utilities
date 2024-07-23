import datetime
from datetime import datetime
import pytz
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, Date, Numeric, Float, TIMESTAMP
import os
from dotenv import load_dotenv
import Init_DB.Python_Scripts.Archieve.DDL_Copy_bkp as DDL_Copy_bkp
import pandas as pd
import psycopg
import log_config
from sqlalchemy.schema import PrimaryKeyConstraint
from pyathena import connect
import sys
import utils
from email_app import send_message
import get_start_date_tables as gsdt

app_logger = log_config.start_log()

if os.path.exists("/app/init_db/Final_DB_Scripts"):
    script_loc = "/app/init_db/Final_DB_Scripts"
    main_loc = "/app/init_db"
    print("Loading Main ENVs")
else:
    py_scr_loc = os.getcwd()
    main_loc = "../"
    script_loc = "../Final_DB_Scripts"
    env_file = "../../aws_common.env"
    try: 
        load_dotenv(env_file)
    except Exception as e:
        print(e)
    else:
        print("Loading Local ENVs")

environment = os.getenv('DEPLOYMENT_TYPE')
# Aggregation Tables
# =================================================
# agg_tbl_b2c = os.getenv("AGG_TBL_B2C")
# b2b_agg_tbl = os.getenv("AGG_TBL_B2B")
# voucher_agg_tbl = os.getenv("AGG_TBL_VOUCHER")
# logistic_agg_tbl = os.getenv("AGG_TBL_LOG")

L1_TBLS, L2_TBLS = utils.generate_req_envs(f_name="./required_envs.txt", env=environment)

schema_name = os.getenv("POSTGRES_SCHEMA")

# Truncation Tables
# =================================================
# bl_tbls_trunc = [os.getenv("DISTRICT_TABLE"), os.getenv("SUB_CATEGORY_TABLE"),
#                  os.getenv("CATEGORY_TABLE"), os.getenv("SELLER_TABLE"),
#                  os.getenv("KEY_DATA_INSIGHTS_TABLE"), os.getenv("KEY_INSIGHTS_SUB_CATEGORY_TABLE"),
#                  os.getenv("KEY_INSIGHTS_SELLER"), os.getenv("SUB_CATEGORY_PENETRATION_TABLE"),
#                  os.getenv("DIM_CATEGORIES"), os.getenv("DIM_DISTRICTS"), os.getenv("DIM_DATES"),
#                  os.getenv("B2B_DISTRICT_TABLE"), os.getenv("B2B_DIM_DATES"),
#                  os.getenv("RV_DISTRICT_TABLE"),os.getenv("LOGISTICS_DISTRICT_TABLE"),
#                  os.getenv("LOG_CATEGORY_TABLE"), os.getenv("MONTHLY_DISTRICT_TABLE"), os.getenv("MONTHLY_PROVIDERS") ]


pg_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"


def get_postgres_connection():
    conn_params = {
        "dbname": os.getenv('POSTGRES_DB'),
        "user": os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "host": os.getenv('POSTGRES_HOST')
    }
    try:
        connection = psycopg.connect(**conn_params)
    except Exception as db_exc:
        app_logger.error(f'Unable to connect to the DB with user {os.getenv("POSTGRES_USER")} on '
                         f'{os.getenv("POSTGRES_HOST")}')
        print(db_exc.args[0])
        raise ConnectionError
    else:
        app_logger.info("Connected")
        return connection


def create_schema(db_conn):
    qry = f"""CREATE SCHEMA if not exists "{os.getenv('POSTGRES_SCHEMA')}" 
    AUTHORIZATION "{os.getenv('POSTGRES_USER')}";"""
    try:
        db_conn.cursor().execute(qry)
    except Exception as e:
        app_logger.debug("Schema not created or not needed to be created. Proceeding... ")
        app_logger.debug(qry)
        app_logger.error(str(e))
        return False
    else:
        db_conn.commit()
        return True


def read_table_struct(tbl_struct_file):
    dict_tbls = {}
    vals = {}
    key = ""
    if os.path.exists(tbl_struct_file):
        app_logger.info("Found file ")
        app_logger.info(tbl_struct_file)
        with open(tbl_struct_file, "r") as f:
            data = f.readlines()
        for line in data:
            if line.startswith(">>>>"):
                vals = {}
                key = os.getenv(line.split(">>>>")[1].split("\n")[0])
                dict_tbls[key] = ""
            elif line.__contains__(":"):
                k1, v1 = line.split(":")[0], line.split(":")[1].split("\n")[0]
                vals[k1] = v1
            else:
                continue
            dict_tbls[key] = vals
        return dict_tbls
    else:
        app_logger.error("File not found ", tbl_struct_file)
        raise FileNotFoundError


def trunc_tbls(tbl_names, db_conn) -> bool:
    qry = f"""TRUNCATE TABLE ONLY "{os.getenv('POSTGRES_SCHEMA')}"."TBL" CONTINUE IDENTITY CASCADE"""
    for x in tbl_names:
        app_logger.debug(f"Now Truncating --> , {x}")
        try:
            to_run = qry.replace("TBL", x)
            app_logger.debug(db_conn.cursor().execute(to_run))
        except Exception as e:
            app_logger.error(f"Couldn't truncate {x}. Check if Table Exists")
            app_logger.debug(str(e))
        else:
            app_logger.debug(f"Truncated {x}")
            db_conn.commit()
    else:
        return True


def get_column_type(data_type, length):
    # Mapping of data types to SQLAlchemy types
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

    # Extract the SQLAlchemy type from the mapping
    if length:
        return type_mapping.get(data_type.lower())(length)
    else:
        return type_mapping.get(data_type.lower())


def create_table(db_url, sch_name, table_name, columns):
    engine = create_engine(db_url)
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=sch_name)
    if sch_name + "." + table_name not in metadata.tables.keys():
        columns_list = []
        for name, column_spec in columns.items():
            # Parse the column specification string into a tuple
            column_spec = eval(column_spec)
            data_type, length = column_spec[0], column_spec[1] if len(column_spec) > 1 else None
            column_type = get_column_type(data_type, length)
            column = Column(name, column_type)
            columns_list.append(column)

        table = Table(table_name, metadata, schema=sch_name, extend_existing=True)
        for column in columns_list:
            table.append_column(column, replace_existing=True)
        if 'id' in columns:
            table.append_constraint(PrimaryKeyConstraint('id', name=f"{table_name}_pkey"))

        metadata.create_all(engine)
        app_logger.info(f"Table '{table_name}' created successfully.")
    else:
        app_logger.debug(f"Table '{table_name}' already exists. Skipping creation.")
    engine.dispose()


def insert_into_pincode(db_url: str):
    engine = create_engine(db_url)
    df = pd.read_parquet(script_loc + "//" + "pc.parquet")
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


def get_mnth_arr(date_range):
    st_dt = date_range[0]
    en_dt = date_range[1]

    st_date = utils.check_date(st_dt)
    en_date = utils.check_date(en_dt)

    if st_date is None or en_date is None:
        return None

    st_mnth, st_yr = utils.check_date(st_dt).month, utils.check_date(st_dt).year
    en_mnth, en_yr = utils.check_date(en_dt).month, utils.check_date(en_dt).year

    app_logger.info(f"{en_mnth}, {st_mnth}")
    months = []
    if st_mnth == en_mnth and st_yr == en_yr:
        return [st_mnth]
    elif en_yr == st_yr and st_mnth != en_mnth:
        for x in range(st_mnth, en_mnth + 1):
            months.append(x)
        return months
    else:
        for x in range(st_mnth, 13):
            months.append(x)
        for x in range(1, en_mnth + 1):
            months.append(x)
        return months


def run_script_on_kw_mnthwise(kws, db_conn, mnth_range):
    for kw in kws:
        app_logger.info("Keyword is ")
        app_logger.info(kw)
        # files = []
        app_logger.info("**************")
        # app_logger.info(files)
        for file in os.listdir(script_loc):
            if file.__contains__(kw):
                app_logger.info(f"Processing {file}")
                fin_scr = utils.read_clean_script(script_loc + "//" + file)
                curr = db_conn.cursor()
                for month in mnth_range:
                    app_logger.info(f"Executing for month {month}")
                    try:
                        app_logger.debug(curr.execute(fin_scr.replace("{MNTH}", str(month))))
                    except Exception as e:
                        app_logger.error(str(e))
                        app_logger.debug(fin_scr)
                        return False
                    else:
                        app_logger.debug("Executed query")
                        app_logger.info("*****************************************************")
    db_conn.commit()
    return True


def run_script_on_kw_daywise(kws, db_conn, date_range):
    for kw in kws:
        app_logger.info("Keyword is ")
        app_logger.info(kw)
        # files = []
        app_logger.info("**************")
        # app_logger.info(files)
        for file in os.listdir(script_loc):
            if file.__contains__(kw):
                app_logger.info(f"Processing {file}")
                fin_scr = utils.read_clean_script(script_loc + "//" + file)
                curr = db_conn.cursor()
                for date in date_range:
                    app_logger.info(f"Executing for {date}")
                    try:
                        app_logger.debug(curr.execute(fin_scr.replace("DATE_PARAM", datetime.strftime(date[0], format="%Y-%m-%d"))))
                    except Exception as e:
                        app_logger.error(str(e))
                        app_logger.debug(fin_scr)
                        return False
                    else:
                        app_logger.debug("Executed query")
                        app_logger.info("*****************************************************")
    db_conn.commit()
    return True


def run_script_on_kw(kws, db_conn):
    for kw in kws:
        app_logger.info("Keyword is ")
        app_logger.info(kw)
        # files = []
        app_logger.info("**************")
        # app_logger.info(files)
        for file in os.listdir(script_loc):
            if file.__contains__(kw):
                app_logger.info(f"Processing {file}")
                fin_scr = utils.read_clean_script(script_loc + "//" + file)
                curr = db_conn.cursor()
                try:
                    app_logger.debug(curr.execute(fin_scr))
                except Exception as e:
                    app_logger.error(str(e))
                    print(e)
                    app_logger.debug(fin_scr)
                    return False
                else:
                    app_logger.debug("Executed query")
                    app_logger.info("*****************************************************")
    db_conn.commit()
    return True


def check_active_connection(conn_string):
    if conn_string.__getstate__()["_closed"]:
        return get_postgres_connection()
    else:
        return conn_string


def write_dq_data():
    try:
        app_logger.info("Connecting to Athena to get Data Quality Report.")
        conn_src_athena = connect(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            s3_staging_dir=os.getenv('S3_STAGING_DIR'),
            region_name=os.getenv('AWS_REGION'),
            schema_name=os.getenv('SCHEMA_NAME')
        )
    except Exception as e:
        print(e.args[0])
        sys.exit()
    else:
        app_logger.info("Connected to Athena Database.")

    try:
        app_logger.info("Connecting to Target Postgres DB.")
        conn_tgt_pg = get_postgres_connection()
    except Exception as e:
        print(e.args[0])
        sys.exit()
    else:
        app_logger.info("Connected to target Postgres Database")

    to_run_src = utils.read_clean_script(script_loc + "//" + "6.1_Sel_DQ_Rep.sql")

    try:
        result = conn_src_athena.cursor().execute(to_run_src).fetchall()
    except Exception as e:
        print(e.args[0])
        sys.exit()
    else:
        app_logger.info("Got Data from Athena.")
        # print(result)

    tgt_qry = utils.read_clean_script(script_loc + "//" + "6.2_INS_DQ_Rep.sql")

    try:
        conn_tgt_pg.cursor().executemany(tgt_qry, result)
    except Exception as e:
        print(e.args[0])
    else:
        conn_tgt_pg.commit()
        app_logger.info("Written the data to db.")
        conn_tgt_pg.close()
        conn_src_athena.close()


def ddl_dml_complete():
    email_log: list = []
    failed_read: dict = {}
    failed_write: dict = {}
    # Generate the latest pincode table from Google Drive
    # app_logger.info("Loading data from Google Drive to get the latest pincode information")
    # load_pc.main()

    time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime(format="%H:%M:%S")
    email_log.append("Started ETL Process at {} (IST)".format(time))

    # Get the Common PSQL connection for the rest of the DB operations. 
    pg_conn = get_postgres_connection()

    # Create schema if necessary. This will not result into failure of run. 
    app_logger.debug("Creating Schema.")
    app_logger.debug(create_schema(pg_conn))

    # Check if tables need to be created. If yes, create them else skip. 
    app_logger.debug("Creating Aggregation tables.")
    table_definitions = read_table_struct(tbl_struct_file=script_loc + "/" + "tbl_names.txt")

    for table_name, columns in table_definitions.items():
        create_table(pg_url, schema_name, table_name, columns)

    # Truncate the first level Postgresql table which will house the Athena aggregation. 
    # Done: This should result is catastrophic failure.

    # Truncate the agg table and postcode table. 
    app_logger.debug("Truncating Aggregation Tables.")
    trunc_status = trunc_tbls([agg_tbl_b2c, os.getenv("TBL_PINCODE"), b2b_agg_tbl, voucher_agg_tbl,
                               logistic_agg_tbl], check_active_connection(pg_conn))
    if trunc_status:
        app_logger.debug(f"Tables {agg_tbl_b2c}, {b2b_agg_tbl} , "
                         f"{voucher_agg_tbl} and Pincode tables truncated successfully.")
    else:
        app_logger.error(f"Error truncating Tables")
        time = datetime.datetime.now()
        email_log.append("Error Truncating tables at. Pipeline Failure. {}".format(time))
        sys.exit()

    # Immediately load the data into postcode table first. 
    app_logger.debug("Loading the data into Postcode Table.")
    app_logger.debug(insert_into_pincode(pg_url))

    app_logger.info("Writing Data to Data Quality Report.")
    write_dq_data()

    # Get the Date range for all aggregation tables.
    dt_ranges_agg = gsdt.get_date_range()
    
    # Load the data into first level agg tables. 
    app_logger.debug("Loading the data into B2C First level Agg Tables.")
    dt_ranges_agg_tbl = DDL_Copy_bkp.run_agg_qry(read_script="2.1_SEL_RET_B2C_FOD.sql",
                                             write_scr="2.2_INS_RET_B2C_FOD.sql",
                                             tbl_name=agg_tbl_b2c,
                                             date_range=dt_ranges_agg[os.getenv("ATH_TBL_B2C")])
    
    app_logger.debug("Loading the data into B2B's First level Agg Tables.")
    dt_ranges_b2b_tbl = DDL_Copy_bkp.run_agg_qry(read_script="2.3_SEL_RET_B2B_FOD.sql",
                                             write_scr="2.4_INS_RET_B2B_FOD.sql",
                                             tbl_name=b2b_agg_tbl,
                                             date_range=dt_ranges_agg[os.getenv("ATH_TBL_B2B")])

    app_logger.debug("Loading the data into Voucher's First level Agg Tables.")
    voucher = DDL_Copy_bkp.run_agg_qry(read_script="2.5_SEL_VOUCHER_FOD.sql",
                                   write_scr="2.6_INS_VOUCHER_FOD.sql",
                                   tbl_name=voucher_agg_tbl,
                                   date_range=dt_ranges_agg[os.getenv("ATH_TBL_VOUCHER")])

    app_logger.debug("Loading the data into Logistic's First level Agg Tables.")
    logistic = DDL_Copy_bkp.run_agg_qry(read_script="2.7_SEL_LOG_FOD.sql",
                                   write_scr="2.8_INS_LOG_FOD.sql",
                                   tbl_name=logistic_agg_tbl,
                                   date_range=dt_ranges_agg[os.getenv("ATH_TBL_LOG")])
    

    # Run the fix for multi-category flag
    # month_arr = get_mnth_arr(dt_ranges_agg_tbl)
    run_script_on_kw_daywise(["_Fix_Multi_"], check_active_connection(pg_conn), dt_ranges_agg[os.getenv("ATH_TBL_B2C")])

    # Run the Zipcode tables.
    # month_arr = get_mnth_arr(dt_ranges_agg_tbl)
    # app_logger.debug(month_arr)
    run_script_on_kw_daywise(["_DEL_zip", "_SEL_zip"], check_active_connection(pg_conn), dt_ranges_agg[os.getenv("ATH_TBL_B2C")])
    
    # The above sequence will take the Max times. 
    # The rest would populate the dashboards. 

    # Truncate tables with Business Logic. 
    app_logger.debug("Truncating tables with Business Logic")
    trunc_status = trunc_tbls(bl_tbls_trunc, check_active_connection(pg_conn))
    if trunc_status:
        app_logger.debug("Business Logic tables truncated successfully.")
    else:
        # TODO: Add additional steps here as countermeasures. 
        app_logger.info("Business Logic tables not truncated. May lead to data duplication.")

    # Populate the KDI and other tables. 
    # "_INS_KEY_"  
    kwags: list[str] = ["_INS_sw", "INS_B2B", "_ad_hoc_", 
                        "_INS_dim_","_VOUCHER_dlo", "_INS_APP_VER_", 
                        "_LVL_ORD_", "_INS_MA_"]
    run_script_on_kw(kwags, check_active_connection(pg_conn))

    # Finally index the whole piece. 
    app_logger.debug("Indexing the data stored in rest of the tables.")
    run_script_on_kw(["_Idx_"], check_active_connection(pg_conn))
    pg_conn.close()

    time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime(format="%H:%M:%S")
    
    email_log.append(f"\n ETL Log for {os.getenv('EMAIL_ENV')}")
    email_log.append(f"\n ETL Process Finished at {format(time)} (IST)")
    email_log.append("\n Data Load got interrupted for \n")
    email_log.append(f"\n Read Errors: \n")
    
    print("Failed Read")
    print(failed_read)
    print(type(failed_read))
    
    for keys in failed_read:
        for value in failed_read[keys]:
            try:
                email_log.append(keys+"--"+value)
            except:
                continue

    email_log.append(f"\n Write Errors: \n")

    for keys in failed_write:
        for value in failed_write[keys]:
            try:
                email_log.append(keys+"--"+value)
            except:
                continue

    print(email_log)
    send_message(email_log)


if __name__ == "__main__":
    ddl_dml_complete()
