import asyncio
import os
import pandas as pd
from glob import glob
from dotenv import load_dotenv
import sys
import env_defs as ed
import utils
       
sem = asyncio.Semaphore(15)
pc_tbl = pd.read_parquet(ed.script_loc+"pc.parquet")


async def process_retail_b2c(tgt_file: str, file_category:str):
    """
    tgt_file: The File to process in Parquet format.
    Pincode file Dataframe will be a global variable.
    file_dump_loc: Where it should be written. 
    
    """
    try:
        tgt_df = pd.read_parquet(tgt_file)
    except:
        print("Parquet files only.")
        return pd.DataFrame()
    
    dt_val = tgt_file.split("query_result_")[1].split("_")[0]
    row_count = tgt_df.shape[0]
    
    if row_count < 1:
        print(f"Empty Dataframe {tgt_df}")
        return pd.DataFrame()
    else:
        print(f"Proceeding with {tgt_file}")
    
    print("Truncating the Pincode columns")
    tgt_df["seller_pincode"] = tgt_df["seller_pincode"].str.strip()
    tgt_df["delivery_pincode"] = tgt_df["delivery_pincode"].str.strip()
    
    tgt_df["total_items"] = tgt_df["total_items"].astype("float")

    print("Fixing NUll Multi-category")
    tgt_df.fillna({"consolidated_categories": "Missing"}, inplace=True)
        
    print("Fixing Multi-category")
    cond = (tgt_df["multi_category_flag"] == "1")
    tgt_df.loc[cond, ["consolidated_categories"]] = "Multi Category"
    
    print("Populating Delivery Stats.")
    final_df = tgt_df.merge(pc_tbl, left_on="delivery_pincode", 
                                    right_on="Pincode", how="left").drop(columns=[
                                        "Pincode","delivery_district","delivery_state","delivery_state_code"]).rename(
                                            columns={"Statename":"delivery_state",
                                                     "Districtname":"delivery_district", 
                                                     "Statecode":"delivery_state_code"})

    print("Populating Seller Stats.")
    final_df = final_df.merge(pc_tbl, left_on="seller_pincode", 
                                              right_on="Pincode", how="left").drop(columns=[
                                                  "Pincode","seller_district","seller_state","seller_state_code"]).rename(
                                                      columns={"Statename":"seller_state",
                                                               "Districtname":"seller_district", 
                                                               "Statecode":"seller_state_code"})
    
    if final_df.shape[0] == row_count:
        print(final_df.shape[0], row_count)
        print("Rows Match, proceeding.\n")
        final_df.to_parquet(f"{ed.processed_files}_{dt_val}_{file_category}.parquet")
    else:
        print("Row Mismatch.")


async def process_retail_b2b(tgt_file: str, file_category:str):
    """
    tgt_file: The File to process in Parquet format.
    Pincode file Dataframe will be a global variable.
    file_dump_loc: Where it should be written. 
    
    """
    try:
        tgt_df = pd.read_parquet(tgt_file)
    except:
        print("Parquet files only.")
        return pd.DataFrame()
    
    dt_val = tgt_file.split("query_result_")[1].split("_")[0]
    row_count = tgt_df.shape[0]
    
    if row_count < 1:
        print(f"Empty Dataframe {tgt_df}")
        return pd.DataFrame()
    else:
        print(f"Proceeding with {tgt_file}")
    
    print("Truncating the Pincode columns")
    tgt_df["delivery_pincode"] = tgt_df["delivery_pincode"].str.strip()
    tgt_df["total_items"] = tgt_df["total_items"].astype("float")

    print("Populating Delivery Stats.")
    final_df = tgt_df.merge(pc_tbl, left_on="delivery_pincode", 
                                    right_on="Pincode", how="left").drop(columns=[
                                        "Pincode","delivery_district","delivery_state","delivery_state_code"]).rename(
                                            columns={"Statename":"delivery_state",
                                                     "Districtname":"delivery_district", 
                                                     "Statecode":"delivery_state_code"})
    if final_df.shape[0] == row_count:
        print(final_df.shape[0], row_count)
        print("Rows Match, proceeding.\n")
        final_df.to_parquet(f"{ed.processed_files}_{dt_val}_{file_category}.parquet")
    else:
        print("Row Mismatch.")
        
        
async def process_logistics(tgt_file: str, file_category:str):
    """
    tgt_file: The File to process in Parquet format.
    Pincode file Dataframe will be a global variable.
    file_dump_loc: Where it should be written. 
    
    """
    try:
        tgt_df = pd.read_parquet(tgt_file)
    except:
        print("Parquet files only.")
        return pd.DataFrame()
    
    dt_val = tgt_file.split("query_result_")[1].split("_")[0]
    row_count = tgt_df.shape[0]
    
    if row_count < 1:
        print(f"Empty Dataframe {tgt_df}")
        return pd.DataFrame()
    else:
        print(f"Proceeding with {tgt_file}")
    
    print("Truncating the Pincode columns")
    tgt_df["pick_up_pincode"] = tgt_df["pick_up_pincode"].str.strip()
    tgt_df["delivery_pincode"] = tgt_df["delivery_pincode"].str.strip()
    
    tgt_df["pick_up_pincode"] = tgt_df["pick_up_pincode"].str.split(".",expand=True)[0]
    tgt_df["delivery_pincode"] = tgt_df["delivery_pincode"].str.split(".",expand=True)[0]
    
    print("Populating Delivery Stats.")
    final_df = tgt_df.merge(pc_tbl, left_on="delivery_pincode", 
                                    right_on="Pincode", how="left").drop(columns=[
                                        "Pincode","delivery_district","delivery_state","delivery_state_code"]).rename(
                                            columns={"Statename":"delivery_state",
                                                     "Districtname":"delivery_district", 
                                                     "Statecode":"delivery_state_code"})
    print("Populating Seller Stats.")
    final_df = final_df.merge(pc_tbl, left_on="pick_up_pincode", 
                                              right_on="Pincode", how="left").drop(columns=[
                                                  "Pincode","seller_district","seller_state","seller_state_code"]).rename(
                                                      columns={"Statename":"seller_state",
                                                               "Districtname":"seller_district", 
                                                               "Statecode":"seller_state_code"})
    if final_df.shape[0] == row_count:
        print(final_df.shape[0], row_count)
        print("Rows Match, proceeding.\n")
        final_df.to_parquet(f"{ed.processed_files}_{dt_val}_{file_category}.parquet")
    else:
        print("Row Mismatch.")
        
        
async def process_voucher(tgt_file: str, file_category:str):
    """
    tgt_file: The File to process in Parquet format.
    Pincode file Dataframe will be a global variable.
    file_dump_loc: Where it should be written. 
    
    """
    try:
        tgt_df = pd.read_parquet(tgt_file)
    except:
        print("Parquet files only.")
        return pd.DataFrame()
    
    dt_val = tgt_file.split("query_result_")[1].split("_")[0]

    row_count = tgt_df.shape[0]
    
    if row_count < 1:
        print(f"Empty Dataframe {tgt_df}")
        return pd.DataFrame()
    else:
        print(f"Proceeding with {tgt_file}")
    
    print("Truncating the Pincode columns")
    tgt_df["delivery_pincode"] = tgt_df["delivery_pincode"].str.strip()
    
    print("Populating Delivery Stats.")
    final_df = tgt_df.merge(pc_tbl, left_on="delivery_pincode", 
                                    right_on="Pincode", how="left").drop(columns=[
                                        "Pincode","delivery_district","delivery_state","delivery_state_code"]).rename(
                                            columns={"Statename":"delivery_state",
                                                     "Districtname":"delivery_district", 
                                                     "Statecode":"delivery_state_code"})
    if final_df.shape[0] == row_count:
        print(final_df.shape[0], row_count)
        print("Rows Match, proceeding.\n")
        final_df.to_parquet(f"{ed.processed_files}_{dt_val}_{file_category}.parquet")
    else:
        print("Row Mismatch.")
        
        
async def read_all_files(files, file_cat: str):
    if file_cat == "retail_b2c":
        tasks = [process_retail_b2c(file, file_cat) for file in files]
        await asyncio.gather(*tasks)
        print("*********** B2C Completed ***********")
    elif file_cat == "retail_b2b":
        tasks = [process_retail_b2b(file, file_cat) for file in files]
        await asyncio.gather(*tasks)
        print("*********** B2B Completed ***********")
    elif file_cat == "voucher":
        tasks = [process_voucher(file, file_cat) for file in files]
        await asyncio.gather(*tasks)
        print("*********** Voucher Completed ***********")
    elif file_cat == "logistics":
        tasks = [process_logistics(file, file_cat) for file in files]
        await asyncio.gather(*tasks)
        print("*********** Logistics Completed ***********")
    else:
        print(f"Invalid option {file_cat}.")


async def async_read_files(locations, file_cat):
    await read_all_files(locations, file_cat)


async def transform_data():
    logistics_files = glob(f'{ed.raw_files}/*shared_open_data_logistics_order*')
    b2b_files = glob(f'{ed.raw_files}/*shared_open_data_b2b_order*')
    b2c_files = glob(f'{ed.raw_files}/*nhm_order_fulfillment_subset_v1*')    
    voucher_files = glob(f'{ed.raw_files}/*shared_open_data_gift_voucher_order*')
    
    val_dict = {"logistics":logistics_files, 
                "retail_b2b":b2b_files, 
                "retail_b2c":b2c_files, 
                "voucher":voucher_files}
    
    main_tasks = [async_read_files(locations= f_grp,
                              file_cat=cat) for cat, f_grp in val_dict.items()]
    await asyncio.gather(*main_tasks)
    

# if __name__ == "__main__":
#     asyncio.run(transform_data())
  
#     print("\nCatalogue of Files before.")
#     utils.catalogue_files_src(ed.raw_files)  
    
#     print("\nCatalogue of Files after.")
#     utils.catalogue_files_tgt(ed.processed_files)
    