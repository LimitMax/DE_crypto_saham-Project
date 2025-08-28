import pyodbc, os, datetime, json
import pandas as pd
from dotenv import load_dotenv

load_dotenv() 

SQL_SERVER    = os.getenv("SQL_SERVER")
SQL_DATABASE  = os.getenv("SQL_DATABASE")
SQL_USERNAME  = os.getenv("SQL_USERNAME")
SQL_PASSWORD  = os.getenv("SQL_PASSWORD")
DRIVER        = '{ODBC Driver 18 for SQL Server}'

def connect_sql():
    return pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

# --- Watermark Management ---
def get_last_success(cursor, symbol):
    cursor.execute("SELECT last_success FROM IngestionMetadata WHERE source=?", symbol)
    row = cursor.fetchone()
    if row and row[0]:
        return row[0]
    else:
        cursor.execute("SELECT MAX(DATEADD(HOUR, hourx, CAST(date AS DATETIME2))) FROM CryptoPrice WHERE crypto=?", symbol)
        row2 = cursor.fetchone()
        return row2[0] if row2 and row2[0] else datetime.datetime(2024,1,1)

def update_last_success(cursor, symbol, new_ts):
    cursor.execute("""
        MERGE IngestionMetadata AS target
        USING (SELECT ? AS source, ? AS last_success) AS src
        ON target.source = src.source
        WHEN MATCHED THEN UPDATE SET last_success=src.last_success, updated_at=GETDATE()
        WHEN NOT MATCHED THEN INSERT (source, last_success) VALUES (src.source, src.last_success);
    """, symbol, new_ts)

# --- Insert Incremental Data ---
def insert_incremental(cursor, df, symbol):
    inserted = 0
    for _, row in df.iterrows():
        r = row.to_dict()

        date_val   = r['date']
        hour_val   = int(r['hourx']) if pd.notna(r['hourx']) else None
        crypto_val = str(r['crypto'])

        open_val   = float(r['Open'])   if pd.notna(r['Open'])   else None
        high_val   = float(r['High'])  if pd.notna(r['High'])   else None
        low_val    = float(r['Low'])   if pd.notna(r['Low'])    else None
        close_val  = float(r['Close']) if pd.notna(r['Close'])  else None
        vol_val    = int(r['Volume'])  if pd.notna(r['Volume']) else None

        cursor.execute("""
            INSERT INTO CryptoPrice (date,hourx,crypto,[Open],[High],[Low],[Close],[Volume])
            SELECT ?,?,?,?,?,?,?,?
            WHERE NOT EXISTS (
                SELECT 1 FROM CryptoPrice WHERE date=? AND hourx=? AND crypto=?
            )
        """,
        date_val, hour_val, crypto_val,
        open_val, high_val, low_val, close_val, vol_val,
        date_val, hour_val, crypto_val)

        inserted += cursor.rowcount

    print(f"📊 {symbol}: {inserted} new rows inserted.")