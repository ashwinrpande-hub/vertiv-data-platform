#!/usr/bin/env python3
"""
vertiv-data-platform/python/data_generator/data_generator.py

Generates synthetic Enterprise Co sales data for 5 source systems:
  SAP (Europe), JDE (Americas), QAD (EMEA), Salesforce, Kafka events

Usage:
  python data_generator.py --rows 5000 --customers 500 --output ./data
  python data_generator.py --rows 5000 --load          (load to Snowflake)
  python data_generator.py --rows 5000 --load --source SAP
"""

import os
import sys
import uuid
import random
import hashlib
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date

try:
    from faker import Faker
except ImportError:
    print("Run: pip install -r requirements.txt")
    sys.exit(1)

fake   = Faker(["en_US", "de_DE", "en_GB", "fr_FR", "ar_SA"])
random.seed(42)

# ── Enterprise Co Product Catalog ────────────────────────────────────
PRODUCTS = [
    {"code": "UPS-GXT5-10K",    "name": "Liebert GXT5 10kVA UPS",          "family": "UPS",        "line": "Liebert GXT5",   "price": 8500},
    {"code": "UPS-GXT5-20K",    "name": "Liebert GXT5 20kVA UPS",          "family": "UPS",        "line": "Liebert GXT5",   "price": 14500},
    {"code": "UPS-EXM-80K",     "name": "Liebert EXM 80kVA UPS",           "family": "UPS",        "line": "Liebert EXM",    "price": 42000},
    {"code": "UPS-APM-120K",    "name": "Enterprise Co APM 120kVA UPS",            "family": "UPS",        "line": "Enterprise Co APM",     "price": 68000},
    {"code": "COOL-DSE-035",    "name": "Liebert DSE 35kW Precision Cool",  "family": "COOLING",    "line": "Liebert DSE",    "price": 18000},
    {"code": "COOL-PCW-030",    "name": "Liebert PCW 30kW Cooling Unit",    "family": "COOLING",    "line": "Liebert PCW",    "price": 22000},
    {"code": "COOL-CRV-25",     "name": "Liebert CRV 25kW Row Cooling",     "family": "COOLING",    "line": "Liebert CRV",    "price": 15000},
    {"code": "PDU-GW-24A",      "name": "Geist Watchdog 24A PDU",           "family": "PDU",        "line": "Geist Watchdog", "price": 1200},
    {"code": "PDU-GW-48A-M",    "name": "Geist Watchdog 48A Metered PDU",   "family": "PDU",        "line": "Geist Watchdog", "price": 2100},
    {"code": "PDU-MPH2-32A",    "name": "MPH2 32A Switched PDU",            "family": "PDU",        "line": "MPH2",           "price": 3200},
    {"code": "RACK-VR-42U",     "name": "Enterprise Co VR 42U Server Rack",        "family": "RACK",       "line": "Enterprise Co VR",      "price": 3500},
    {"code": "RACK-VR-48U-OF",  "name": "Enterprise Co VR 48U Open Frame Rack",    "family": "RACK",       "line": "Enterprise Co VR",      "price": 4200},
    {"code": "MON-TRELLIS-ENT", "name": "Trellis Enterprise Platform",      "family": "MONITORING", "line": "Trellis",        "price": 35000},
    {"code": "MON-TRELLIS-SITE","name": "Trellis Site Manager",             "family": "MONITORING", "line": "Trellis",        "price": 12000},
    {"code": "SVC-PM-ANNUAL",   "name": "Preventive Maintenance Annual",    "family": "SERVICES",   "line": "Services",       "price": 5000},
    {"code": "SVC-REMOTE-MON",  "name": "Remote Monitoring 1yr Service",    "family": "SERVICES",   "line": "Services",       "price": 3000},
]

ORDER_STATUSES = ["OPEN","CONFIRMED","PROCESSING","IN_DELIVERY","DELIVERED","INVOICED","CANCELLED","ON_HOLD"]
STATUS_WEIGHTS = [5,    15,          10,           15,           25,          20,         7,          3]
ORDER_TYPES    = ["STANDARD","RUSH_ORDER","CONTRACT","SERVICE_ORDER","SPARE_PARTS","DEMO"]
INDUSTRIES     = ["DATA_CENTER","TELECOM","HEALTHCARE","FINANCE","GOVERNMENT","RETAIL","MANUFACTURING","EDUCATION","COLOCATION"]

REGIONS = {
    "EUROPE": {
        "erp": "SAP",
        "countries":   ["DEU","GBR","FRA","NLD","SWE","ITA","ESP","BEL","AUT","CHE","POL","DNK"],
        "currencies":  ["EUR","GBP","CHF","SEK","DKK"],
        "fx_to_usd":   {"EUR":1.08,"GBP":1.27,"CHF":1.10,"SEK":0.096,"DKK":0.145},
    },
    "AMERICAS": {
        "erp": "JDE",
        "countries":   ["USA","CAN","BRA","MEX","ARG","CHL","COL","PER"],
        "currencies":  ["USD","CAD","BRL","MXN"],
        "fx_to_usd":   {"USD":1.00,"CAD":0.74,"BRL":0.20,"MXN":0.059},
    },
    "EMEA": {
        "erp": "QAD",
        "countries":   ["ARE","SAU","ZAF","EGY","KEN","NGA","KWT","QAT","IND","SGP"],
        "currencies":  ["USD","AED","SAR","ZAR","INR","SGD"],
        "fx_to_usd":   {"USD":1.00,"AED":0.272,"SAR":0.267,"ZAR":0.054,"INR":0.012,"SGD":0.74},
    },
}


def sha256_hk(source: str, pk: str) -> str:
    """Matches Snowflake: SHA2(UPPER(source)||'|'||UPPER(pk), 256)"""
    return hashlib.sha256(f"{source.upper()}|{pk.upper()}".encode()).hexdigest()


def gen_customers(n: int, region: str) -> pd.DataFrame:
    cfg  = REGIONS[region]
    rows = []
    for i in range(n):
        cid = f"{cfg['erp'][:3]}-CUST-{i+1:06d}"
        rows.append({
            "CUSTOMER_ID":    cid,
            "CUSTOMER_NAME":  fake.company(),
            "CUSTOMER_TYPE":  random.choice(["ENTERPRISE","DISTRIBUTOR","DIRECT","RESELLER","OEM"]),
            "INDUSTRY_CODE":  random.choice(INDUSTRIES),
            "STREET":         fake.street_address(),
            "CITY":           fake.city(),
            "POSTAL_CODE":    fake.postcode()[:10],
            "COUNTRY_CODE":   random.choice(cfg["countries"]),
            "PHONE":          fake.phone_number()[:20],
            "EMAIL":          fake.company_email(),
            "CREDIT_LIMIT":   round(random.uniform(50_000, 5_000_000), 2),
            "PAYMENT_TERMS":  random.choice(["NET30","NET60","NET90","NET45","IMMEDIATE"]),
            "SOURCE_SYSTEM":  cfg["erp"],
            "SOURCE_REGION":  region,
            "BATCH_ID":       str(uuid.uuid4()),
        })
    return pd.DataFrame(rows)


def gen_orders(customers: pd.DataFrame, region: str,
               start: date, end: date, n: int):
    cfg = REGIONS[region]
    orders, items = [], []
    days_range = (end - start).days

    for i in range(n):
        erp        = cfg["erp"]
        oid        = f"{erp[:3]}-ORD-{i+1:08d}"
        odate      = start + timedelta(days=random.randint(0, days_range))
        cust       = customers.sample(1).iloc[0]
        currency   = random.choice(cfg["currencies"])
        fx         = cfg["fx_to_usd"].get(currency, 1.0)
        status     = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        n_lines    = random.randint(1, 8)
        total_net  = 0.0

        for lnum in range(1, n_lines + 1):
            prod     = random.choice(PRODUCTS)
            qty      = random.randint(1, 20)
            disc     = random.choices([0,5,10,15,20,25], weights=[40,20,15,10,10,5], k=1)[0]
            uprice   = prod["price"] * random.uniform(0.88, 1.12)
            line_amt = qty * uprice * (1 - disc / 100)
            total_net += line_amt
            items.append({
                "ORDER_ID":       oid,
                "LINE_NUMBER":    lnum,
                "PRODUCT_CODE":   prod["code"],
                "PRODUCT_NAME":   prod["name"],
                "PRODUCT_FAMILY": prod["family"],
                "QUANTITY":       qty,
                "UNIT_OF_MEASURE":"EA",
                "UNIT_PRICE":     round(uprice, 4),
                "DISCOUNT_PCT":   disc,
                "LINE_AMOUNT":    round(line_amt, 4),
                "LINE_AMOUNT_USD":round(line_amt * fx, 4),
                "CURRENCY_CODE":  currency,
                "LINE_STATUS":    status,
                "SOURCE_SYSTEM":  erp,
                "SOURCE_REGION":  region,
                "BATCH_ID":       str(uuid.uuid4()),
            })

        orders.append({
            "ORDER_ID":       oid,
            "ORDER_DATE":     odate.isoformat(),
            "DELIVERY_DATE":  (odate + timedelta(days=random.randint(7, 60))).isoformat(),
            "CUSTOMER_ID":    cust["CUSTOMER_ID"],
            "ORDER_TYPE":     random.choice(ORDER_TYPES),
            "ORDER_STATUS":   status,
            "CURRENCY_CODE":  currency,
            "NET_AMOUNT":     round(total_net, 4),
            "NET_AMOUNT_USD": round(total_net * fx, 4),
            "TAX_AMOUNT":     round(total_net * fx * 0.08, 4),
            "DISCOUNT_PCT":   random.choices([0,5,10,15], weights=[50,25,15,10], k=1)[0],
            "COUNTRY_CODE":   cust["COUNTRY_CODE"],
            "SOURCE_SYSTEM":  erp,
            "SOURCE_REGION":  region,
            "BATCH_ID":       str(uuid.uuid4()),
        })

    return pd.DataFrame(orders), pd.DataFrame(items)


def gen_salesforce_opps(customers: pd.DataFrame, n: int) -> pd.DataFrame:
    stages = [
        ("Prospecting",10),("Qualification",20),("Needs Analysis",30),
        ("Value Proposition",40),("Id. Decision Makers",60),
        ("Perception Analysis",70),("Proposal/Price Quote",75),
        ("Negotiation/Review",90),("Closed Won",100),("Closed Lost",0),
    ]
    rows = []
    for i in range(n):
        stage, prob = random.choice(stages)
        cdate = date.today() + timedelta(days=random.randint(-180, 180))
        cust  = customers.sample(1).iloc[0]
        rows.append({
            "OPP_ID":       f"SFDC-OPP-{i+1:07d}",
            "OPP_NAME":     f"{cust['CUSTOMER_NAME']} — {random.choice(PRODUCTS)['family']} Project",
            "ACCOUNT_ID":   cust["CUSTOMER_ID"],
            "STAGE_NAME":   stage,
            "AMOUNT":       round(random.uniform(10_000, 2_000_000), 2),
            "PROBABILITY":  prob,
            "CLOSE_DATE":   cdate.isoformat(),
            "OWNER_ID":     f"USER-{random.randint(1,50):04d}",
            "LEAD_SOURCE":  random.choice(["Web","Partner","Cold Call","Event","Referral","Existing Account"]),
            "IS_WON":       stage == "Closed Won",
            "IS_CLOSED":    stage in ("Closed Won","Closed Lost"),
            "REGION":       random.choice(["EUROPE","AMERICAS","EMEA"]),
            "SOURCE_SYSTEM":"SALESFORCE",
            "SOURCE_REGION":"GLOBAL",
            "BATCH_ID":     str(uuid.uuid4()),
        })
    return pd.DataFrame(rows)


def gen_kafka_events(orders: pd.DataFrame, n: int) -> pd.DataFrame:
    types   = ["ORDER_CREATED","ORDER_UPDATED","ORDER_SHIPPED","ORDER_DELIVERED","ORDER_CANCELLED"]
    weights = [35, 30, 20, 10, 5]
    rows = []
    for i in range(n):
        o     = orders.sample(1).iloc[0]
        etype = random.choices(types, weights=weights, k=1)[0]
        etime = datetime.now() - timedelta(minutes=random.randint(0, 2880))
        rows.append({
            "EVENT_ID":        str(uuid.uuid4()),
            "KAFKA_TOPIC":     "vertiv.sales.orders.events",
            "KAFKA_PARTITION": random.randint(0, 11),
            "KAFKA_OFFSET":    i * 100 + random.randint(0, 99),
            "EVENT_TIMESTAMP": etime.strftime("%Y-%m-%d %H:%M:%S"),
            "EVENT_TYPE":      etype,
            "SOURCE_SYSTEM":   o["SOURCE_SYSTEM"],
            "EVENT_PAYLOAD":   str({
                "order_id":   o["ORDER_ID"],
                "customer_id":o["CUSTOMER_ID"],
                "event":      etype,
                "amount_usd": float(o["NET_AMOUNT_USD"]),
                "ts":         etime.isoformat(),
            }),
        })
    return pd.DataFrame(rows)


def save_csv(df: pd.DataFrame, path: Path, name: str) -> Path:
    fpath = path / name
    df.to_csv(fpath, index=False)
    print(f"  ✓ {name}: {len(df):,} rows")
    return fpath


def load_to_snowflake(df: pd.DataFrame, table: str, conn) -> int:
    """PUT temp CSV to stage then COPY INTO target table."""
    import snowflake.connector
    import tempfile; tmp = os.path.join(tempfile.gettempdir(), f"{table.replace('.','_').replace(' ','_')}.csv")
    df.to_csv(tmp, index=False)
    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{tmp} @ENTERPRISE_RAW.STAGE.ENTERPRISE_TMP OVERWRITE=TRUE AUTO_COMPRESS=TRUE")
        cur.execute(f"""
            COPY INTO {table}
            FROM @ENTERPRISE_RAW.STAGE.ENTERPRISE_TMP/{os.path.basename(tmp)}.gz
            FILE_FORMAT = (FORMAT_NAME = 'ENTERPRISE_RAW.STAGE.CSV_FORMAT')
            ON_ERROR='CONTINUE'
        """)
        n = cur.fetchone()[0]
        print(f"  ✓ Loaded {n:,} rows → {table}")
        return n
    except Exception as e:
        print(f"  ✗ Failed loading {table}: {e}")
        return 0
    finally:
        cur.close()
        try:
            os.remove(tmp)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Enterprise Co Synthetic Data Generator")
    parser.add_argument("--rows",       type=int, default=5_000, help="Sales orders per region")
    parser.add_argument("--customers",  type=int, default=500,   help="Customers per region")
    parser.add_argument("--output",     default="./data",         help="Output directory")
    parser.add_argument("--load",       action="store_true",      help="Load CSV into Snowflake")
    parser.add_argument("--source",     default="ALL",
                        choices=["SAP","JDE","QAD","SALESFORCE","KAFKA","ALL"],
                        help="Generate only this source (default ALL)")
    parser.add_argument("--start-date", default="2021-01-01")
    parser.add_argument("--end-date",   default=date.today().isoformat())
    args = parser.parse_args()

    out   = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end_date,   "%Y-%m-%d").date()

    print(f"\n🏭  Enterprise Co Data Generator")
    print(f"    rows={args.rows:,}  customers={args.customers:,}  {start}→{end}\n")

    all_customers: dict = {}
    all_orders:    dict = {}

    run_regions = {
        "SAP": ["EUROPE"],
        "JDE": ["AMERICAS"],
        "QAD": ["EMEA"],
        "ALL": ["EUROPE","AMERICAS","EMEA"],
    }.get(args.source, ["EUROPE","AMERICAS","EMEA"])

    for region in run_regions:
        erp = REGIONS[region]["erp"]
        print(f"📦  {erp} ({region})")
        custs  = gen_customers(args.customers, region)
        ords, itms = gen_orders(custs, region, start, end, args.rows)

        save_csv(custs, out, f"{erp.lower()}_customers.csv")
        save_csv(ords,  out, f"{erp.lower()}_sales_orders.csv")
        save_csv(itms,  out, f"{erp.lower()}_order_items.csv")

        all_customers[region] = custs
        all_orders[region]    = ords

    if args.source in ("SALESFORCE","ALL"):
        print("\n📦  Salesforce CRM")
        all_c  = pd.concat(list(all_customers.values())) if all_customers else gen_customers(100,"EUROPE")
        sfdc   = gen_salesforce_opps(all_c, max(args.rows // 2, 500))
        save_csv(sfdc, out, "salesforce_opportunities.csv")

    if args.source in ("KAFKA","ALL"):
        print("\n📦  Kafka Events")
        all_o  = pd.concat(list(all_orders.values())) if all_orders else pd.DataFrame()
        if not all_o.empty:
            kafka = gen_kafka_events(all_o, max(args.rows // 3, 200))
            save_csv(kafka, out, "kafka_order_events.csv")

    total = sum(len(v) for v in all_customers.values()) + \
            sum(len(v) for v in all_orders.values())
    print(f"\n✅  Generated {total:,} rows → {out}\n")

    if args.load:
        print("🔄  Loading to Snowflake...")
        for var in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD"]:
            if not os.environ.get(var):
                print(f"✗ Missing env var: {var}")
                sys.exit(1)

        import snowflake.connector
        conn = snowflake.connector.connect(
            account   = os.environ["SNOWFLAKE_ACCOUNT"],
            user      = os.environ["SNOWFLAKE_USER"],
            password  = os.environ["SNOWFLAKE_PASSWORD"],
            warehouse = "INGEST_WH",
            database  = "ENTERPRISE_RAW",
            role      = "PLATFORM_ADMIN",
        )
        # Create temp stage
        conn.cursor().execute("CREATE STAGE IF NOT EXISTS ENTERPRISE_RAW.STAGE.ENTERPRISE_TMP")

        load_map = {
            "ENTERPRISE_RAW.SAP_EUROPE.CUSTOMERS":             out / "sap_customers.csv",
            "ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS":          out / "sap_sales_orders.csv",
            "ENTERPRISE_RAW.JDE_AMERICAS.CUSTOMERS":           out / "jde_customers.csv",
            "ENTERPRISE_RAW.JDE_AMERICAS.SALES_ORDERS":        out / "jde_sales_orders.csv",
            "ENTERPRISE_RAW.QAD_EMEA.CUSTOMERS":               out / "qad_customers.csv",
            "ENTERPRISE_RAW.QAD_EMEA.SALES_ORDERS":            out / "qad_sales_orders.csv",
            "ENTERPRISE_RAW.SALESFORCE_GLOBAL.OPPORTUNITIES":  out / "salesforce_opportunities.csv",
        }
        for table, csv_path in load_map.items():
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                load_to_snowflake(df, table, conn)
            else:
                print(f"  ⚠ File not found, skipping: {csv_path.name}")

        conn.close()
        print("\n✅  Snowflake load complete!")


if __name__ == "__main__":
    main()
