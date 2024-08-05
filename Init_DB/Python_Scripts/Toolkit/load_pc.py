import os.path
import pandas as pd
import sys

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from dotenv import load_dotenv

src_loc = "../../"

try:
    load_dotenv("../../../.env")
except:
    raise FileNotFoundError(".env")
else:
    print("Env File Loaded.")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
pc_tbl = os.getenv("SAMPLE_SPREADSHEET_ID")
SAMPLE_RANGE_NAME = "OD Pincode Final!A1:D"
tgt_dest = r"../../../"


def get_pincode_tbl():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (sheet.values()
                  .get(spreadsheetId=pc_tbl, range=SAMPLE_RANGE_NAME)
                  .execute())
        values = result.get("values", [])

        if not values:
            print("No data found.")
            return
        else:
            return values
    except HttpError as err:
        print(err)


def main():
    # print(os.getcwd())
    df_tbl = pd.DataFrame(get_pincode_tbl())
    df_tbl.columns = [x for x in df_tbl.iloc[0]]
    df_tbl.drop(index=0, axis=1, inplace=True)
    dest_file1 = tgt_dest + "Init_DB/Final_DB_Scripts/" + "pc.parquet"
    print(dest_file1)
    if os.path.exists(dest_file1):
        print("Pincode File Exists. Regenerating.")
        os.remove(dest_file1)
    else:
        print("Pincode file not found. Generating now.")
    df_tbl.to_parquet(dest_file1, index=False, compression='lz4')
    print("Total Rows = ", df_tbl.shape[0])


if __name__ == "__main__":
    main()
