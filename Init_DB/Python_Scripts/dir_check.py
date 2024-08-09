import os
from datetime import datetime
from dotenv import load_dotenv
import env_defs as ed


load_dotenv(ed.env_file)
base_loc = os.getenv("DATA_DUMP_LOC")
dt_today = str(datetime.today().date())
tgt_folder = f"{base_loc}{str(dt_today)}"


def check_folder():
    print(dt_today)
    print(tgt_folder)

    try:
        if os.path.exists(tgt_folder):
            print("Dump already exists for today.")
        else:
            print("Creating folder.")
            print(tgt_folder)
            os.mkdir(tgt_folder)
    except Exception as e:
        print(e)
        return ""
    else:
        print("Folder operation successful.")
        return tgt_folder


if __name__ == "__main__":
    check_folder()