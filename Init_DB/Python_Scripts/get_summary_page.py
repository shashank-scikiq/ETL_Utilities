from psycopg import connect
from datetime import datetime
import env_defs as ed
import os
import sys


def main():  
    pg_url = f"postgresql://{ed.user}:{ed.passwd}@{ed.host}:{ed.port}/{os.getenv('DB_NAME_TARGET')}"
    conn = ""
    
    try:
        conn = connect(pg_url)
    except Exception as e:
        raise e
    else:
        print("Connected to the Database.")


    with conn.cursor() as cur:
        result = cur.execute(f"SELECT * from {os.getenv('SCHEMA_TARGET')}.{os.getenv('TABLE_MONTH')}")
        data = result.fetchall()


    data_list = []

    for domain, date, transactions in data:
        data_list.append([transactions, domain, 
                            date.strftime(format="%Y-%m-%d"),transactions])
        

    for x in data_list:
        print(f'[{x[0]}, \n"{x[1]}",\n"{x[2]}", \n{x[3]}],')
        

if __name__ == "__main__":
    main()