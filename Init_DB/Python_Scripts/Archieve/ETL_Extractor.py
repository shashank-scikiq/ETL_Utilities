import Init_DB.Python_Scripts.Extract_SRC as exc
import asyncio
import os 
from get_start_date_tables import get_date_ranges
from datetime import date, timedelta
import Init_DB.Python_Scripts.Extract_SRC as es
from utils import timing_decorator

db_dump_loc = "/home/sraj/Documents/OD_DB_Files/Raw_DB_Files/"

async def sleep_print(table_name, script_to_run):
    await asyncio.sleep(1)
    print(table_name, " ", script_to_run)


def get_last_three_days(days_range):
  """
  This function returns a list of the last 3 days' dates (including today).
  """
  today = date.today()
  last_three_days = [today - timedelta(days=i) for i in range(days_range)]
  return last_three_days


@timing_decorator
async def query_athena_db():
    
    print("Extracting the Data right now.")
    
    src_tbl_dict = {
        os.getenv("ATH_TBL_B2C") : "ATH_TBL_B2C.sql",
        os.getenv("ATH_TBL_B2B") : "ATH_TBL_B2B.sql",
        os.getenv("ATH_TBL_VOUCHER") : "ATH_TBL_VOUCHER.sql",
        os.getenv("ATH_TBL_LOG") : "ATH_TBL_LOG.sql"
    }
    
    print("Fetching Date Ranges.")
    date_ranges = get_date_ranges() 
    print("Date ranges obtained.")
    
    print(src_tbl_dict)
    tasks = [es.dump_data(tbl_name= key, dump_loc= db_dump_loc,
                          date_range= date_ranges[key],
                          read_script_file= value) for key, value in src_tbl_dict.items()]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(query_athena_db())
    