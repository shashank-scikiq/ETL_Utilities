import log_config
import os
import sys
from datetime import datetime, date, time
import re
from functools import wraps

app_logger = log_config.start_log()
import env_defs
from glob import glob

"""
read_file = Read a text file and return it's contents as output list.

return_env_dict = Read a .env file and return the dictionary containing 
the key value pair.

chk_req_envs = Read the required_env.txt file and convert the same 

generate_req_envs = Generate the environment variables, based on the 
.env file name passed. This accepts a base location where the file will be 
output, if file type is chosen as an output. 

check_tbl_defs = Check if all the required tables from the environment files
have been defined in the table_names.txt. 

read_clean_script = Reads a sql script and converts any reference of environment 
variable with its value from the environment file. 

check_date = Accepts a string and converts it into date format. The 
input value must be in YYYY-MM-DD format. 

"""

source_params = {
    'host': f"{os.getenv('SRC_POSTGRES_HOST')}",
    'port': f"{os.getenv('SRC_POSTGRES_PORT')}",
    'database': f"{os.getenv('SRC_POSTGRES_DB')}",
    'user': f"{os.getenv('SRC_POSTGRES_USER')}",
    'password': f"{os.getenv('SRC_POSTGRES_PWD')}",
    'timeout': 60,
}


def read_file(f_name: str) -> list[str]:
    """
    f_name = The file name which needs to be read along with
    the location.
    
    Returns a list of contents.
    """
    file_contents: list[str] = []
    if os.path.exists(f_name):
        app_logger.info(f"Found File {f_name}.")
        try:
            with open(f_name, 'r') as f:
                file_contents = f.readlines()
        except Exception as e:
            app_logger.info(e.args[0])
            sys.exit()
        else:
            print("File read successfully.")
    else:
        print("File {} not Found. Exiting. ".format(f_name))
        sys.exit()
    return file_contents


def return_env_dict(env_file: list[str]) -> dict:
    """
    env_file = Environment file and the location in which this file is 
    located.
    
    Reads the environment file and returns a dictionary of the environment 
    file in key-value pair dictionary. 
    
    The format should be ENV="value". 
    
    """
    env_f_name = read_file(env_file)
    src_env_dict: dict = {}
    for line in env_f_name:
        if not (line.startswith("#") or line.startswith('\n')):
            key = line.split("=")[0]
            try:
                value = line.split("=")[1]
            except:
                value = line.split("=")[1]
            src_env_dict[key] = value

    return src_env_dict


def generate_req_envs(f_name: str, env: str):
    """
    f_name = The complete file name with location for the required_env file.
    
    env = "stage" / "prod". defines whether the environment files taken be for stage or prod. 
    
    Returns two sets of lists, one containing the L1 agg tables, the second containing L2 agg tables, 
        
    """

    app_logger.info("Reading the Environment file.")
    env_dict = read_file(f_name)

    L1_kws = ["AGG_TBL"]
    prod_kws = ["TBL"]
    stage_kws = ["CATEGORY_TBL", "INSIGHTS_TBL", "KEY_INSIGHTS", "SUB_CATEGORY", "CATEGORIES_TBL"]
    exclude_kws = ["ATH", "OD_DQ_TBL"]

    L1_TBLS = [key.split("\n")[0] for key in env_dict if any(kw in key for kw in L1_kws)]

    L2_TBLS = [key.split("\n")[0] for key in env_dict if
               (env == "prod" and any(kw in key for kw in prod_kws) and not any(kw in key for kw in stage_kws)
                and not any(kw in key for kw in exclude_kws)
                and not any(kw in key for kw in L1_kws))
               or (env == "stage" and any(kw in key for kw in prod_kws) and not any(kw in key for kw in exclude_kws)
                   and not any(kw in key for kw in L1_kws))]
    return L1_TBLS, L2_TBLS


def chk_req_envs(req_env: str) -> bool:
    """
    req_env = A file with location that contains the required environment 
    variables. 
    
    This file must be in a list format and the file with location should be 
    passed. 
    
    """
    required_envs = read_file(req_env)
    for key in required_envs:
        if key.split("\n")[0] not in os.environ.keys():
            print(f"Required Environment Variable [{key}] not declared.")
            print("Exiting.")
            return False
    else:
        print("All the environment variables loaded.")
        return True


def check_tbl_defs(tbl_def_file: str, req_envs: list) -> None:
    """
    tbl_def_file: The file with complete path, 
    which has table definitions for creation. 
    
    req_envs_file: A which contains all definitions of required tables. 
    
    """

    tbl_arr = []
    src_tbl_names = read_file(tbl_def_file)

    for line in src_tbl_names:
        if line.startswith(">>>>"):
            tbl_arr.append(line.split(">>>>")[1].split('\n')[0])

    req_tbls = [x for x in req_envs if (x.__contains__("TBL")) and (not x.__contains__("ATH"))]

    for x in req_tbls:
        if x not in tbl_arr:
            print(f"Definition of {x} not present in tbl_names.")
            print("Table Array = ", tbl_arr)
            print("Required Tables = ", req_tbls)
            break
    else:
        print("All Table Definitions present.")


def read_clean_script(f_name: str, env_file: str) -> str:
    """
    f_name = The file name which needs to be read. This will be a 
    sql script that will be read by this function. It will replace
    any environment file reference with its value. 
    
    env_file = File containing the environment variables.     
    """

    incl_kws = ["POSTGRES_DB", "_SCHEMA", "ATH", "TBL", "VER_", "START"]
    # all_envs = return_env_dict(env_file)
    all_envs = read_file(env_file)
    # repl_dict = {key: val.strip() for key, val in all_envs.items() if any(kw in key for kw in incl_kws)}
    repl_dict = {key.split('\n')[0]: os.getenv(key.split('\n')[0]) for key in all_envs if any(kw in key for kw in incl_kws)}
    # print(repl_dict)
    to_run = read_file(f_name)
    val = "".join(to_run)

    for key, value in repl_dict.items():
        try:
            val = re.sub(rf'\b{re.escape(key)}\b', value, val)
        except Exception as e:
            app_logger.error("Error while replacing the values.")
            print(key, value)
            print(str(e))
            sys.exit()
    return val


def check_date(dt_val) -> datetime.date:
    '''Enter string in YYYY-MM-DD Format. 
    Checks if a date is valid. '''

    if isinstance(dt_val, date):
        return dt_val
    else:
        try:
            dt = datetime.strptime(dt_val, "%Y-%m-%d").date()
            return dt
        except ValueError:
            app_logger.error(
                "Use YYYY-MM-DD format with the right date value")
            return ""


def timing_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        end_time = datetime.now()
        print(f"Function {func.__name__} executed in {end_time - start_time} seconds")
        return result

    return wrapper


def catalogue_files_src(file_loc: str):
    dict_loc = {"b2b_files": glob(f'{file_loc}/*shared_open_data_b2b_order*'),
                "logistic_files": glob(f'{file_loc}/*shared_open_data_logistics_order*'),
                "retail_files": glob(f'{file_loc}/*nhm_order_fulfillment_subset_v1*'),
                "voucher_files": glob(f'{file_loc}/*shared_open_data_gift_voucher_order*')}
    for key in dict_loc.keys():
        print(key, "--", len(dict_loc[key]))


def catalogue_files_tgt(file_loc: str):
    dict_loc = {"b2b_files": glob(f'{file_loc}/*retail_b2b*'),
                "logistic_files": glob(f'{file_loc}/*logistics*'),
                "retail_files": glob(f'{file_loc}/*retail_b2c*'),
                "voucher_files": glob(f'{file_loc}/*voucher*')}
    for key in dict_loc.keys():
        print(key, "--", len(dict_loc[key]))


async def find_parquet_files(directory):
    parquet_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))
    return parquet_files
