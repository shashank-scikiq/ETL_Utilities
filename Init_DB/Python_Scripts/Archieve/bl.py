import log_config
import os
import utils
import psycopg
from dotenv import load_dotenv

app_logger = log_config.start_log()
script_loc = "../Final_DB_Scripts/"
env_file = "../../aws_common.env"
load_dotenv(env_file)

def check_active_connection(conn_string):
    return get_postgres_connection()
   
    
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
                fin_scr = utils.read_clean_script(script_loc + "//" + file, env_file)
                print(fin_scr)
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

pg_conn = get_postgres_connection()


# kwags: list[str] = ["_INS_sw", "INS_B2B", "_ad_hoc_", 
#                         "_INS_dim_","_VOUCHER_dlo", "_INS_APP_VER_", 
#                         "_LVL_ORD_", "_INS_MA_"]
kwags: list[str] = ["_INS_dim_"]
run_script_on_kw(kwags, check_active_connection(pg_conn))

# Finally index the whole piece. 
app_logger.debug("Indexing the data stored in rest of the tables.")
run_script_on_kw(["_Idx_"], check_active_connection(pg_conn))
