import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

for line in open(".env").readlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.strip().split("=", 1)
        os.environ[k] = v

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse="INGEST_WH",
    role="PLATFORM_ADMIN",
)

# Industry code replacements to fit VARCHAR(10)
INDUSTRY_MAP = {
    "DATA_CENTER": "DATACENTER",
    "HEALTHCARE":  "HEALTHCR",
    "GOVERNMENT":  "GOVT",
    "MANUFACTURING": "MANUFACT",
    "EDUCATION":   "EDUCATN",
    "COLOCATION":  "COLOC",
}

# Order type replacements to fit VARCHAR(10)
ORDER_TYPE_MAP = {
    "SPARE_PARTS":  "SPARE",
    "RUSH_ORDER":   "RUSH",
    "SERVICE_ORDER": "SERVICE",
    "STANDARD":     "STANDARD",
    "CONTRACT":     "CONTRACT",
    "DEMO":         "DEMO",
}

tables = {
    "ENTERPRISE_RAW.SAP_EUROPE.CUSTOMERS":    "data/sap_customers.csv",
    "ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS": "data/sap_sales_orders.csv",
    "ENTERPRISE_RAW.JDE_AMERICAS.SALES_ORDERS": "data/jde_sales_orders.csv",
}

for full_table, csv_path in tables.items():
    db, schema, tbl = full_table.split(".")
    df = pd.read_csv(csv_path)
    df.columns = [c.upper() for c in df.columns]

    # Apply replacements
    if "INDUSTRY_CODE" in df.columns:
        df["INDUSTRY_CODE"] = df["INDUSTRY_CODE"].replace(INDUSTRY_MAP)
        df["INDUSTRY_CODE"] = df["INDUSTRY_CODE"].str[:10]
    if "ORDER_TYPE" in df.columns:
        df["ORDER_TYPE"] = df["ORDER_TYPE"].replace(ORDER_TYPE_MAP)
        df["ORDER_TYPE"] = df["ORDER_TYPE"].str[:10]
    if "CUSTOMER_TYPE" in df.columns:
        df["CUSTOMER_TYPE"] = df["CUSTOMER_TYPE"].str[:20]

    # Get Snowflake columns
    cur = conn.cursor()
    cur.execute(f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{tbl}' ORDER BY ORDINAL_POSITION")
    sf_cols = [row[0] for row in cur.fetchall()]
    cur.close()

    skip = ["LOAD_ID","LOAD_TIMESTAMP","LOAD_DATE","IS_DELETED","CHANGE_TYPE","RAW_RECORD"]
    valid_cols = [c for c in sf_cols if c in df.columns and c not in skip]
    df = df[valid_cols]

    # Truncate all strings to safe length
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str[:200]

    try:
        conn.cursor().execute(f"USE DATABASE {db}")
        conn.cursor().execute(f"USE SCHEMA {schema}")
        success, nchunks, nrows, _ = write_pandas(
            conn, df, tbl,
            database=db, schema=schema,
            auto_create_table=False,
            overwrite=False
        )
        print(f"  OK {nrows:,} rows -> {full_table}")
    except Exception as e:
        print(f"  FAIL {full_table}: {e}")

conn.close()
print("All done!")
