import os
import env_defs as ev
from dotenv import load_dotenv
import asyncio

try:
    load_dotenv(ev.env_file)
except Exception as e:
    raise e


async def check_files(dir_name: str):
    counter = 0
    date = []  # Get the names by pattern matching.
    for _, _, files in os.walk(dir_name):
        for file in files:
            if file.endswith(".parquet"):
                counter += 1
    print(f"The count of files on {dir_name} is {counter}.")
    return counter


# async def  get_counter(dirs: list):
#     tasks = [check_files(dir) for dir in dirs]
#     print(f"Processing the dir {dirs}")
#     results = await asyncio.gather(*tasks)
#     return results


async def file_ops(base_loc: str):
    total_orders = base_loc + "/total_orders/"
    raw_files = base_loc + "/Raw_DB_Files/"
    processed_files = base_loc + "/Processed_Files"
    print(f"Total Orders loc is ", total_orders)
    print(f"Dump loc is ", raw_files)
    print(f"Dump loc is ", processed_files)
    dir_locs = [total_orders, raw_files, processed_files]
    tasks = [check_files(x) for x in dir_locs]
    res = await asyncio.gather(*tasks)
    print(res)


if __name__ == "__main__":
    asyncio.run(file_ops("/home/sraj/Documents/OD_DB_Files_8_July-2024"))
