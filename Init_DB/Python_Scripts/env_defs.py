import os
import utils
from dotenv import load_dotenv

# Load environment variables

def load_envs():
    if os.path.exists("/app/init_db/Final_DB_Scripts/"):
        if os.getenv("POSTGRES_SCHEMA") == "ec2_dev" or os.getenv(
                "POSTGRES_SCHEMA") == "ec2_stage" or os.getenv(
                "POSTGRES_SCHEMA") == "ec2_prod":
            env_file = "/mnt/env/ec2_common.env"
        elif os.getenv("POSTGRES_SCHEMA") == "stage":
            env_file = "/mnt/env/aws_stage.env"
        elif os.getenv("POSTGRES_SCHEMA") == "prod":
            env_file = "/mnt/env/aws_prod.env"
        else:
            env_file = "/mnt/env/.env"

        script_loc = "/app/init_db/Final_DB_Scripts/"
        py_scr_loc = "/app/init_db/Python_Scripts/"
        main_loc = "/app/init_db/"
        dump_loc = '/mnt/data/'
        raw_files = f"{dump_loc}Raw_DB_Files/"
        processed_files = f"{dump_loc}Processed_Files/"
        total_orders = f"{dump_loc}total_orders/"
        req_envs = f"{py_scr_loc}/required_envs.txt"
        pincode_file = f"{script_loc}pc.parquet"
        tbl_names = f"{script_loc}tbl_names.txt"

        print("Loading Main ENVs")
        print(f'Schema is {os.getenv("POSTGRES_SCHEMA")}')
    else:
        py_scr_loc = os.getcwd()
        main_loc = "../../"
        script_loc = f"{main_loc}/Init_DB/Final_DB_Scripts/"
        env_file = f"{main_loc}.env"

        try:
            load_dotenv(env_file)
        except Exception as e:
            print(e)
        else:
            print("Loading Local ENVs")

        dump_loc = f"{os.getenv('DATA_DUMP_LOC')}"
        raw_files = f"{dump_loc}/Raw_DB_Files/"
        processed_files = f"{dump_loc}/Processed_Files/"
        total_orders = f"{dump_loc}/total_orders/"
        req_envs = f"{py_scr_loc}/required_envs.txt"
        pincode_file = f"{script_loc}pc.parquet"
        tbl_names = f"{script_loc}tbl_names.txt"
        print(f'Schema is {os.getenv("POSTGRES_SCHEMA")}')

    user = os.getenv('POSTGRES_USER')
    passwd = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    db = os.getenv('POSTGRES_DB')
    schema_name = os.getenv("POSTGRES_SCHEMA")
    environment = os.getenv('DEPLOYMENT_TYPE')

    asyncpg_url = f"postgresql+asyncpg://{user}:{passwd}@{host}:{port}/{db}"
    scr_env = os.getenv("DEPLOYMENT_TYPE")

    INS_TBL_MAPPING_SQL = {
        # L2 Tables Prod
        'DISTRICT_TBL': f"{script_loc}DISTRICT_TBL.sql",
        'SELLER_TBL': f"{script_loc}SELLER_TBL.sql",
        'DIM_DISTRICTS_TBL': f"{script_loc}DIM_DISTRICTS_TBL.sql",
        'DIM_DATES_TBL': f"{script_loc}DIM_DATES_TBL.sql",
        'B2B_DISTRICT_TBL': f"{script_loc}B2B_DISTRICT_TBL.sql",
        'B2B_DIM_DATES_TBL': f"{script_loc}B2B_DIM_DATES_TBL.sql",
        'RV_DISTRICT_TBL': f"{script_loc}RV_DISTRICT_TBL.sql",
        'LOGISTICS_DISTRICT_TBL': f"{script_loc}LOGISTICS_DISTRICT_TBL.sql",
        'LOGISTICS_PROVIDERS_TBL': f"{script_loc}LOGISTICS_PROVIDERS_TBL.sql",
        'LOGISTICS_MONTHLY_PROVIDERS_TBL': f"{script_loc}LOGISTICS_MONTHLY_PROVIDERS_TBL.sql",
        'MONTHLY_DISTRICT_TBL': f"{script_loc}MONTHLY_DISTRICT_TBL.sql",
        'MONTHLY_PROVIDERS_TBL': f"{script_loc}MONTHLY_PROVIDERS_TBL.sql",
        'ONDC_DASHBOARD_VERSION_TBL': f"{script_loc}ONDC_DASHBOARD_VERSION_TBL.sql",
        # L2 Tables Non Prod Section
        'SUB_CATEGORY_TBL': f"{script_loc}SUB_CATEGORY_TBL.sql",
        'CATEGORY_TBL': f"{script_loc}CATEGORY_TBL.sql",
        'KEY_DATA_INSIGHTS_TBL': f"{script_loc}KEY_DATA_INSIGHTS_TBL.sql",
        'KEY_INSIGHTS_SUB_CATEGORY_TBL': f"{script_loc}KEY_INSIGHTS_SUB_CATEGORY_TBL.sql",
        'KEY_INSIGHTS_SELLER_TBL': f"{script_loc}KEY_INSIGHTS_SELLER_TBL.sql",
        'SUB_CATEGORY_PENETRATION_TBL': f"{script_loc}KEY_DATA_INSIGHTS_TBL.sql",
        'DIM_CATEGORIES_TBL': f"{script_loc}DIM_CATEGORIES_TBL.sql",
        'LOG_CATEGORY_TBL': f"{script_loc}LOG_CATEGORY_TBL.sql"
    }

    seq1 = ["ONDC_DASHBOARD_VERSION_TBL", "B2B_DIM_DATES_TBL", "B2B_DISTRICT_TBL", "CATEGORY_TBL", "DIM_CATEGORIES_TBL",
            "DIM_DATES_TBL", "DISTRICT_TBL", "KEY_INSIGHTS_SELLER",
            "SUB_CATEGORY_TBL", "LOG_CATEGORY_TBL", "LOGISTICS_DISTRICT_TBL",
            "LOGISTICS_PROVIDERS_TBL", "DIM_DISTRICTS_TBL"]

    seq2 = ["KEY_DATA_INSIGHTS_TBL", "KEY_INSIGHTS_SUB_CATEGORY_TABLE", "LOGISTICS_MONTHLY_PROVIDERS_TBL",
            "RV_DISTRICT_TBL"]

    seq3 = ["MONTHLY_DISTRICT_TBL"]

    # L1_TBLS , L2_TBLS = utils.generate_req_envs(req_envs, scr_env)


# =================================================================



# =================================================================

if os.path.exists("/app/init_db/Final_DB_Scripts/"):
    if os.getenv("POSTGRES_SCHEMA") == "ec2_dev" or os.getenv(
            "POSTGRES_SCHEMA") == "ec2_stage" or os.getenv(
            "POSTGRES_SCHEMA") == "ec2_prod":
        env_file = "/mnt/env/ec2_common.env"
    elif os.getenv("POSTGRES_SCHEMA") == "stage":
        env_file = "/mnt/env/aws_stage.env"
    elif os.getenv("POSTGRES_SCHEMA") == "prod":
        env_file = "/mnt/env/aws_prod.env"
    else:
        env_file = "/mnt/env/.env"

    script_loc = "/app/init_db/Final_DB_Scripts/"
    py_scr_loc = "/app/init_db/Python_Scripts/"
    main_loc = "/app/init_db/"
    dump_loc = '/mnt/data/'
    raw_files = f"{dump_loc}Raw_DB_Files/"
    processed_files = f"{dump_loc}Processed_Files/"
    total_orders = f"{dump_loc}total_orders/"
    req_envs = f"{py_scr_loc}/required_envs.txt"
    pincode_file = f"{script_loc}pc.parquet"
    tbl_names = f"{script_loc}tbl_names.txt"

    print("Loading Main ENVs")
    print(f'Schema is {os.getenv("POSTGRES_SCHEMA")}')
else:
    py_scr_loc = os.getcwd()
    main_loc = "../../"
    script_loc = f"{main_loc}/Init_DB/Final_DB_Scripts/"
    env_file = f"{main_loc}.env"

    try:
        load_dotenv(env_file)
    except Exception as e:
        print(e)
    else:
        print("Loading Local ENVs")

    dump_loc = f"{os.getenv('DATA_DUMP_LOC')}"
    raw_files = f"{dump_loc}/Raw_DB_Files/"
    processed_files = f"{dump_loc}/Processed_Files/"
    total_orders = f"{dump_loc}/total_orders/"
    req_envs = f"{py_scr_loc}/required_envs.txt"
    pincode_file = f"{script_loc}pc.parquet"
    tbl_names = f"{script_loc}tbl_names.txt"
    print(f'Schema is {os.getenv("POSTGRES_SCHEMA")}')

user = os.getenv('POSTGRES_USER')
passwd = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
db = os.getenv('POSTGRES_DB')
schema_name = os.getenv("POSTGRES_SCHEMA")
environment = os.getenv('DEPLOYMENT_TYPE')

asyncpg_url = f"postgresql+asyncpg://{user}:{passwd}@{host}:{port}/{db}"
scr_env = os.getenv("DEPLOYMENT_TYPE")

INS_TBL_MAPPING_SQL = {
    # L2 Tables Prod
    'DISTRICT_TBL': f"{script_loc}DISTRICT_TBL.sql",
    'SELLER_TBL': f"{script_loc}SELLER_TBL.sql",
    'DIM_DISTRICTS_TBL': f"{script_loc}DIM_DISTRICTS_TBL.sql",
    'DIM_DATES_TBL': f"{script_loc}DIM_DATES_TBL.sql",
    'B2B_DISTRICT_TBL': f"{script_loc}B2B_DISTRICT_TBL.sql",
    'B2B_DIM_DATES_TBL': f"{script_loc}B2B_DIM_DATES_TBL.sql",
    'RV_DISTRICT_TBL': f"{script_loc}RV_DISTRICT_TBL.sql",
    'LOGISTICS_DISTRICT_TBL': f"{script_loc}LOGISTICS_DISTRICT_TBL.sql",
    'LOGISTICS_PROVIDERS_TBL': f"{script_loc}LOGISTICS_PROVIDERS_TBL.sql",
    'LOGISTICS_MONTHLY_PROVIDERS_TBL': f"{script_loc}LOGISTICS_MONTHLY_PROVIDERS_TBL.sql",
    'MONTHLY_DISTRICT_TBL': f"{script_loc}MONTHLY_DISTRICT_TBL.sql",
    'MONTHLY_PROVIDERS_TBL': f"{script_loc}MONTHLY_PROVIDERS_TBL.sql",
    'ONDC_DASHBOARD_VERSION_TBL': f"{script_loc}ONDC_DASHBOARD_VERSION_TBL.sql",
    # L2 Tables Non Prod Section
    'SUB_CATEGORY_TBL': f"{script_loc}SUB_CATEGORY_TBL.sql",
    'CATEGORY_TBL': f"{script_loc}CATEGORY_TBL.sql",
    'KEY_DATA_INSIGHTS_TBL': f"{script_loc}KEY_DATA_INSIGHTS_TBL.sql",
    'KEY_INSIGHTS_SUB_CATEGORY_TBL': f"{script_loc}KEY_INSIGHTS_SUB_CATEGORY_TBL.sql",
    'KEY_INSIGHTS_SELLER_TBL': f"{script_loc}KEY_INSIGHTS_SELLER_TBL.sql",
    'SUB_CATEGORY_PENETRATION_TBL': f"{script_loc}KEY_DATA_INSIGHTS_TBL.sql",
    'DIM_CATEGORIES_TBL': f"{script_loc}DIM_CATEGORIES_TBL.sql",
    'LOG_CATEGORY_TBL': f"{script_loc}LOG_CATEGORY_TBL.sql"
}

seq1 = ["ONDC_DASHBOARD_VERSION_TBL", "B2B_DIM_DATES_TBL", "B2B_DISTRICT_TBL", "CATEGORY_TBL", "DIM_CATEGORIES_TBL",
        "DIM_DATES_TBL", "DISTRICT_TBL", "KEY_INSIGHTS_SELLER",
        "SUB_CATEGORY_TBL", "LOG_CATEGORY_TBL", "LOGISTICS_DISTRICT_TBL",
        "LOGISTICS_PROVIDERS_TBL", "DIM_DISTRICTS_TBL"]

seq2 = ["KEY_DATA_INSIGHTS_TBL", "KEY_INSIGHTS_SUB_CATEGORY_TABLE", "LOGISTICS_MONTHLY_PROVIDERS_TBL",
        "RV_DISTRICT_TBL"]

seq3 = ["MONTHLY_DISTRICT_TBL"]

# L1_TBLS , L2_TBLS = utils.generate_req_envs(req_envs, scr_env)
