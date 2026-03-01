import pandas as pd
import os
from sqlalchemy import create_engine, types
import pyodbc

SERVER = r"WELKIE"
DATABASE = "eCommerce_Tiki"
FOLDER_PATH = r"datasets\cleaned_data"

# Check driver
print(pyodbc.drivers())

connection_url = (
    f"mssql+pyodbc://{SERVER}/{DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

engine = create_engine(connection_url)

for file_name in os.listdir(FOLDER_PATH):
    if file_name.endswith(".csv"):
        table_name = file_name.replace(".csv", "")
        file_path = os.path.join(FOLDER_PATH, file_name)

        print(f"Importing: {file_name}...")

        df = pd.read_csv(file_path, encoding="utf-8-sig")

        dtype_mapping = {}
        for col in df.select_dtypes(include=['object', 'string']).columns:
            dtype_mapping[col] = types.NVARCHAR(None)

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping
        )

        print(f"Imported {table_name}")

print("Finished.")