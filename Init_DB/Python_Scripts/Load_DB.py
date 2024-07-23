import asyncio
import asyncpg
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
from dotenv import load_dotenv
import os
import sys
import log_config
from glob import glob
from utils import timing_decorator
import env_defs as ed


tgt_params = {
    'host': f"{os.getenv('POSTGRES_HOST')}",
    'port': int(f"{os.getenv('POSTGRES_PORT')}"),
    'database': f"{os.getenv('POSTGRES_DB')}",
    'user': f"{os.getenv('POSTGRES_USER')}",
    'password': f"{os.getenv('POSTGRES_PASSWORD')}",
    'timeout': 60
}

src_logger = log_config.start_log()
tgt_schema = os.getenv("POSTGRES_SCHEMA")


# Semaphore to limit concurrent connections
semaphore = asyncio.Semaphore(10)


async def load_logistic_data(parquet_file: str, tgt_table: str):
    conn = None
    try:
        async with semaphore:
            conn = await asyncpg.connect(**tgt_params)
            table = pq.read_table(parquet_file)
            
            # Convert the table to a pandas DataFrame for easier manipulation
            df = table.to_pandas()
            
            # # Handle any necessary datetime conversions
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

            # Prepare the data for insertion
            records = df.to_dict(orient='records')
            records = [tuple(record.values()) for record in records]

            # Insert records using copy_records_to_table
            async with conn.transaction():
                await conn.copy_records_to_table(
                    table_name=tgt_table,
                    schema_name=tgt_schema,
                    records=records,
                    columns=[
                        'bap_id', 'bpp_id', 'provider_id', 'order_id', 'transaction_id', 'item_id',
                        'fulfillment_status', 'order_date', 'domain', 'f_agent_assigned_at_date',
                        'Log_Ondc_Status', 'network_retail_order_id', 'shipment_type', 'pick_up_pincode',
                        'delivery_pincode', 'network_retail_order_category', 'on_confirm_sync_response',
                        'on_confirm_error_code', 'on_confirm_error_message', 'delivery_district',
                        'delivery_state', 'delivery_state_code', 'seller_district', 'seller_state', 
                        'seller_state_code']
                )
            
            print(f"{datetime.now()}: Loaded {parquet_file} into Postgresql table {tgt_table}")

    except Exception as e:
        print(f"{datetime.now()}: !!! Error loading {parquet_file} into Postgresql: {e}")

    finally:
        if conn:
            await conn.close()
            # print(f"{datetime.now()}: Closed connection to Postgresql")
            

async def load_retail_b2c_data(parquet_file: str, tgt_table: str):
    conn = None
    try:
        async with semaphore:
            conn = await asyncpg.connect(**tgt_params)
            table = pq.read_table(parquet_file)
            
            # Convert the table to a pandas DataFrame for easier manipulation
            df = table.to_pandas()
            
            # # Handle any necessary datetime conversions
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            df["total_items"] = df["total_items"].astype("float")
            df["multi_category_flag"] = df["multi_category_flag"].astype("int")

            # Prepare the data for insertion
            records = df.to_dict(orient='records')
            records = [tuple(record.values()) for record in records]

            # Insert records using copy_records_to_table
            async with conn.transaction():
                await conn.copy_records_to_table(
                    table_name=tgt_table,
                    schema_name=tgt_schema,
                    records=records,
                    columns=['network_order_id', 'sub_category', 'seller_np', 'total_items',
                            'domain', 'provider_key', 'order_status', 'seller_pincode',
                            'delivery_pincode', 'order_date', 'consolidated_categories',
                            'multi_category_flag', 'delivery_district','delivery_state',
                            'delivery_state_code','seller_district','seller_state','seller_state_code']
                )
            
            print(f"{datetime.now()}: Loaded {parquet_file} into Postgresql table {tgt_table}")

    except Exception as e:
        print(f"{datetime.now()}: !!! Error loading {parquet_file} into Postgresql: {e}")

    finally:
        if conn:
            await conn.close()
            # print(f"{datetime.now()}: Closed connection to Postgresql")
            
            
async def load_retail_b2b_data(parquet_file: str, tgt_table: str):
    conn = None
    try:
        async with semaphore:
            conn = await asyncpg.connect(**tgt_params)
            table = pq.read_table(parquet_file)
            
            # Convert the table to a pandas DataFrame for easier manipulation
            df = table.to_pandas()
            
            # # Handle any necessary datetime conversions
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            df["total_items"] = df["total_items"].astype("float")
            
            # Prepare the data for insertion
            records = df.to_dict(orient='records')
            records = [tuple(record.values()) for record in records]

            # Insert records using copy_records_to_table
            async with conn.transaction():
                await conn.copy_records_to_table(
                    table_name=tgt_table,
                    schema_name=tgt_schema,
                    records=records,
                    columns=['network_order_id', 'seller_np', 'total_items', 'domain',
                            'provider_key', 'order_status', 'delivery_pincode', 'order_date',
                            'delivery_district','delivery_state', 'delivery_state_code'])
            
            print(f"{datetime.now()}: Loaded {parquet_file} into Postgresql table {tgt_table}")

    except Exception as e:
        print(f"{datetime.now()}: !!! Error loading {parquet_file} into Postgresql: {e}")

    finally:
        if conn:
            await conn.close()
            # print(f"{datetime.now()}: Closed connection to Postgresql")
            
            
async def load_voucher_data(parquet_file: str, tgt_table: str):
    conn = None
    try:
        async with semaphore:
            conn = await asyncpg.connect(**tgt_params)
            table = pq.read_table(parquet_file)
            
            # Convert the table to a pandas DataFrame for easier manipulation
            df = table.to_pandas()
            
            # # Handle any necessary datetime conversions
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            df["total_items"] = df["total_items"].astype("float")

            # Prepare the data for insertion
            records = df.to_dict(orient='records')
            records = [tuple(record.values()) for record in records]

            # Insert records using copy_records_to_table
            async with conn.transaction():
                await conn.copy_records_to_table(
                    table_name=tgt_table,
                    schema_name=tgt_schema,
                    records=records,
                    columns=['network_order_id', 'provider_key', 'total_items', 'domain',
                            'order_status', 'delivery_pincode', 'order_date','delivery_district',
                            'delivery_state', 'delivery_state_code'])
            
            print(f"{datetime.now()}: Loaded {parquet_file} into Postgresql table {tgt_table}")

    except Exception as e:
        print(f"{datetime.now()}: !!! Error loading {parquet_file} into Postgresql: {e}")

    finally:
        if conn:
            await conn.close()
            # print(f"{datetime.now()}: Closed connection to Postgresql")
            

@timing_decorator   
async def populate_data():
    try:
        
        # :TODO: Shift this to utils 
        
        logistics_files = glob(f'{ed.processed_files}/*logistics*')
        b2b_files = glob(f'{ed.processed_files}/*retail_b2b*')
        b2c_files = glob(f'{ed.processed_files}/*retail_b2c*')    
        voucher_files = glob(f'{ed.processed_files}/*voucher*')
                
        tasks = []
        tasks += [load_retail_b2c_data(parquet_file=p_file, tgt_table=os.getenv("AGG_TBL_B2C")) for p_file in b2c_files]
        tasks += [load_retail_b2b_data(parquet_file=p_file, tgt_table=os.getenv("AGG_TBL_B2B")) for p_file in b2b_files]
        tasks += [load_voucher_data(parquet_file=p_file, tgt_table=os.getenv("AGG_TBL_VOUCHER")) for p_file in voucher_files]
        tasks += [load_logistic_data(parquet_file=p_file, tgt_table=os.getenv("AGG_TBL_LOG")) for p_file in logistics_files]
        
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"{datetime.now()}: Error in main function: {e}")
        

# if __name__ == "__main__":
#     asyncio.run(populate_data())