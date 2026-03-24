#!/usr/bin/env python3
"""
vertiv-data-platform/python/snowpark/feature_engineering.py

Point-in-time correct ML feature engineering using Snowpark
Writes to VERTIV_ANALYTICS.ML_FEATURES + AI vector tables

Usage:
  python feature_engineering.py
  python feature_engineering.py --as-of-date 2024-06-30
"""

import os
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("features")

try:
    from snowflake.snowpark import Session
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.types import FloatType, IntegerType, StringType
except ImportError:
    print("Run: pip install -r requirements.txt")
    sys.exit(1)


def get_session() -> Session:
    for var in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD"]:
        if not os.environ.get(var):
            log.error(f"Missing env var: {var}")
            sys.exit(1)
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": "ML_WH",
        "database":  "VERTIV_ANALYTICS",
        "schema":    "ML_FEATURES",
        "role":      "VERTIV_ML_ENGINEER",
    }).create()


# ── Feature computation ───────────────────────────────────────
def compute_customer_revenue_features(session: Session,
                                      as_of: date,
                                      version: int = 1) -> int:
    """
    Compute revenue features per customer as of `as_of` date.
    STRICT PIT: all predicates use < as_of, never <=.
    No data from as_of or later is ever included.
    """
    d_str = as_of.isoformat()
    log.info(f"  Computing customer revenue features  as_of={d_str}")

    # All Silver orders strictly before as_of_date
    orders = (
        session.table("VERTIV_CURATED.SALES.SALES_ORDER")
        .filter(F.col("IS_CURRENT") == True)
        .filter(F.col("IS_QUARANTINED") == False)
        .filter(F.col("ORDER_DATE") < F.lit(d_str))  # STRICT: < not <=
    )
    n = orders.count()
    if n == 0:
        log.warning(f"    No orders before {d_str} — skipping")
        return 0

    # Lagged revenue windows (all computed relative to as_of_date)
    feats = orders.group_by("CUSTOMER_HK").agg(
        # Revenue windows
        F.sum(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 7,
            F.col("NET_AMOUNT_USD")
        ).otherwise(0)).alias("REV_L7D_USD"),

        F.sum(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 30,
            F.col("NET_AMOUNT_USD")
        ).otherwise(0)).alias("REV_L30D_USD"),

        F.sum(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 90,
            F.col("NET_AMOUNT_USD")
        ).otherwise(0)).alias("REV_L90D_USD"),

        F.sum(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 365,
            F.col("NET_AMOUNT_USD")
        ).otherwise(0)).alias("REV_L365D_USD"),

        F.sum(F.when(
            (F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                        F.to_timestamp(F.lit(d_str))) > 365) &
            (F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                        F.to_timestamp(F.lit(d_str))) <= 730),
            F.col("NET_AMOUNT_USD")
        ).otherwise(0)).alias("REV_PREV_YEAR_USD"),

        # Order counts
        F.count(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 30, F.lit(1)
        )).alias("ORDER_COUNT_L30"),

        F.count(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 90, F.lit(1)
        )).alias("ORDER_COUNT_L90"),

        F.count(F.when(
            F.datediff("day", F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))) <= 365, F.lit(1)
        )).alias("ORDER_COUNT_L365"),

        # Value stats
        F.max("NET_AMOUNT_USD").alias("MAX_ORDER_VALUE"),
        F.min("NET_AMOUNT_USD").alias("MIN_ORDER_VALUE"),
        F.max("ORDER_DATE").alias("LAST_ORDER_DATE"),
        F.count("*").alias("TOTAL_ORDERS"),
    )

    # Derived features
    feats = (
        feats
        .with_column("DAYS_SINCE_LAST_ORDER",
            F.datediff("day",
                       F.col("LAST_ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.to_timestamp(F.lit(d_str))))
        .with_column("ORDER_FREQUENCY_L365",
            (F.col("ORDER_COUNT_L365") / F.lit(12.0)).cast(FloatType()))
        .with_column("AVG_ORDER_VALUE_L90",
            F.when(F.col("ORDER_COUNT_L90") > 0,
                   F.col("REV_L90D_USD") / F.col("ORDER_COUNT_L90")
            ).otherwise(F.lit(0.0)))
        .with_column("PRODUCT_MIX_HHI", F.lit(0.25).cast(FloatType()))  # simplified
        .with_column("AS_OF_DATE",      F.lit(d_str))
        .with_column("FEATURE_VERSION", F.lit(version))
        # Churn risk heuristic (0-1): high if > 180 days since last order
        .with_column("CHURN_RISK_SCORE",
            F.greatest(F.lit(0.0),
            F.least(F.lit(1.0),
                (F.col("DAYS_SINCE_LAST_ORDER").cast(FloatType()) / F.lit(365.0)) *
                F.lit(0.7) +
                F.when(F.col("REV_L90D_USD") == 0, F.lit(0.3)).otherwise(F.lit(0.0))
            )).cast(FloatType()))
        # Upsell propensity: growing recent revenue = high propensity
        .with_column("UPSELL_PROPENSITY",
            F.greatest(F.lit(0.0),
            F.least(F.lit(1.0),
                F.when(F.col("REV_L30D_USD") > (F.col("REV_L90D_USD") / F.lit(3.0)),
                       F.lit(0.75)
                ).otherwise(F.lit(0.25))
            )).cast(FloatType()))
    )

    final = feats.select(
        "CUSTOMER_HK","AS_OF_DATE","FEATURE_VERSION",
        "REV_L7D_USD","REV_L30D_USD","REV_L90D_USD",
        "REV_L365D_USD","REV_PREV_YEAR_USD",
        "ORDER_COUNT_L30","ORDER_COUNT_L90","ORDER_COUNT_L365",
        "AVG_ORDER_VALUE_L90","MAX_ORDER_VALUE","MIN_ORDER_VALUE",
        "DAYS_SINCE_LAST_ORDER","ORDER_FREQUENCY_L365",
        "PRODUCT_MIX_HHI","CHURN_RISK_SCORE","UPSELL_PROPENSITY",
    )

    count = final.count()
    final.write.mode("append").save_as_table(
        "VERTIV_ANALYTICS.ML_FEATURES.CUSTOMER_REVENUE_FEATURES"
    )
    log.info(f"    ✅  Wrote {count:,} feature rows  as_of={d_str}")
    return count


def generate_customer_summaries(session: Session) -> int:
    """Write text narrative for each current customer into AI.CUSTOMER_360_VECTORS."""
    log.info("  Generating Customer 360 narratives...")

    sql = """
    MERGE INTO VERTIV_ANALYTICS.AI.CUSTOMER_360_VECTORS tgt
    USING (
      SELECT
        c.MASTER_CUSTOMER_HK                                      AS CUSTOMER_HK,
        c.CUSTOMER_NAME,
        'Vertiv customer: '   || c.CUSTOMER_NAME
        || '. Industry: '     || COALESCE(c.INDUSTRY, 'Unknown')
        || '. Region: '       || COALESCE(c.REGION, 'Unknown')
        || '. Segment: '      || COALESCE(c.SEGMENT, 'Unknown')
        || '. Revenue 12mo: $'|| TO_CHAR(COALESCE(f.REV_L365D_USD, 0), '999,999,999')
        || '. Orders 90d: '   || COALESCE(f.ORDER_COUNT_L90, 0)::VARCHAR
        || '. Churn risk: '   || CASE
             WHEN COALESCE(f.CHURN_RISK_SCORE,0) > 0.7 THEN 'HIGH'
             WHEN COALESCE(f.CHURN_RISK_SCORE,0) > 0.4 THEN 'MEDIUM'
             ELSE 'LOW' END                                        AS CUSTOMER_SUMMARY,
        ARRAY_CONSTRUCT(c.INDUSTRY, c.SEGMENT)                    AS TAGS,
        ARRAY_CONSTRUCT(c.COUNTRY_CODE)                           AS TOP_COUNTRIES,
        CASE
          WHEN COALESCE(f.REV_L365D_USD,0) > 1000000 THEN 'ENTERPRISE'
          WHEN COALESCE(f.REV_L365D_USD,0) > 100000  THEN 'MID_MARKET'
          ELSE 'SMB' END                                           AS REVENUE_BAND
      FROM VERTIV_ANALYTICS.BI.DIM_CUSTOMER c
      LEFT JOIN VERTIV_ANALYTICS.ML_FEATURES.CUSTOMER_REVENUE_FEATURES f
        ON  c.MASTER_CUSTOMER_HK = f.CUSTOMER_HK
        AND f.AS_OF_DATE   = CURRENT_DATE()
        AND f.FEATURE_VERSION = 1
      WHERE c.IS_CURRENT = TRUE
    ) src ON tgt.CUSTOMER_HK = src.CUSTOMER_HK
    WHEN MATCHED THEN UPDATE SET
      CUSTOMER_NAME    = src.CUSTOMER_NAME,
      CUSTOMER_SUMMARY = src.CUSTOMER_SUMMARY,
      REVENUE_BAND     = src.REVENUE_BAND,
      TAGS             = src.TAGS,
      TOP_COUNTRIES    = src.TOP_COUNTRIES,
      LAST_UPDATED     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
      (CUSTOMER_HK, CUSTOMER_NAME, CUSTOMER_SUMMARY,
       REVENUE_BAND, TAGS, TOP_COUNTRIES, LAST_UPDATED)
    VALUES
      (src.CUSTOMER_HK, src.CUSTOMER_NAME, src.CUSTOMER_SUMMARY,
       src.REVENUE_BAND, src.TAGS, src.TOP_COUNTRIES, CURRENT_TIMESTAMP())
    """
    try:
        result = session.sql(sql).collect()
        log.info(f"    ✅  Customer narratives merged")
    except Exception as e:
        log.warning(f"    Customer narrative merge skipped (no DIM_CUSTOMER yet): {e}")
        return 0

    # Generate embeddings (requires Cortex enabled on account)
    embed_sql = """
    UPDATE VERTIV_ANALYTICS.AI.CUSTOMER_360_VECTORS
    SET SUMMARY_VECTOR = SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'e5-base-v2',
        CUSTOMER_SUMMARY
    )
    WHERE SUMMARY_VECTOR IS NULL
    AND   CUSTOMER_SUMMARY IS NOT NULL
    """
    try:
        session.sql(embed_sql).collect()
        log.info("    ✅  Cortex embeddings generated (e5-base-v2, 768-dim)")
    except Exception as e:
        log.warning(f"    Cortex embed skipped (check Cortex is enabled in account): {e}")

    try:
        return session.table("VERTIV_ANALYTICS.AI.CUSTOMER_360_VECTORS").count()
    except Exception:
        return 0


def write_model_predictions_sample(session: Session):
    """Write sample model predictions (demo data if real model not trained)."""
    log.info("  Writing sample model predictions...")
    sql = """
    INSERT INTO VERTIV_ANALYTICS.ML_FEATURES.MODEL_PREDICTIONS
      (MODEL_NAME, MODEL_VERSION, ENTITY_TYPE, ENTITY_HK,
       AS_OF_DATE, PREDICTION_LABEL, PREDICTION_SCORE,
       PREDICTION_BAND, CONFIDENCE_INTERVAL)
    SELECT
      'CHURN_RISK'         AS MODEL_NAME,
      'v1.0'               AS MODEL_VERSION,
      'CUSTOMER'           AS ENTITY_TYPE,
      CUSTOMER_HK          AS ENTITY_HK,
      AS_OF_DATE,
      CASE WHEN CHURN_RISK_SCORE > 0.7 THEN 'CHURN_HIGH'
           WHEN CHURN_RISK_SCORE > 0.4 THEN 'CHURN_MEDIUM'
           ELSE 'CHURN_LOW' END        AS PREDICTION_LABEL,
      CHURN_RISK_SCORE                 AS PREDICTION_SCORE,
      CASE WHEN CHURN_RISK_SCORE > 0.7 THEN 'HIGH'
           WHEN CHURN_RISK_SCORE > 0.4 THEN 'MEDIUM'
           ELSE 'LOW' END              AS PREDICTION_BAND,
      0.05                             AS CONFIDENCE_INTERVAL
    FROM VERTIV_ANALYTICS.ML_FEATURES.CUSTOMER_REVENUE_FEATURES
    WHERE AS_OF_DATE = CURRENT_DATE()
    AND   FEATURE_VERSION = 1
    AND   CUSTOMER_HK NOT IN (
      SELECT ENTITY_HK FROM VERTIV_ANALYTICS.ML_FEATURES.MODEL_PREDICTIONS
      WHERE MODEL_NAME='CHURN_RISK' AND AS_OF_DATE=CURRENT_DATE()
    )
    """
    try:
        session.sql(sql).collect()
        log.info("    ✅  Model predictions written")
    except Exception as e:
        log.warning(f"    Predictions skipped: {e}")


def run_pipeline(session: Session, as_of: date, version: int = 1):
    """Run full feature + AI pipeline for a given as_of date."""
    log.info(f"\n{'='*55}")
    log.info(f"Feature Pipeline  as_of={as_of.isoformat()}  version={version}")
    log.info(f"{'='*55}")

    count = compute_customer_revenue_features(session, as_of, version)
    if count > 0:
        generate_customer_summaries(session)
        write_model_predictions_sample(session)
    log.info("✅  Feature pipeline complete\n")


def main():
    import argparse
    p = argparse.ArgumentParser(description="Vertiv Feature Engineering Pipeline")
    p.add_argument("--as-of-date", default=None, help="YYYY-MM-DD (default: today)")
    p.add_argument("--days-back",  type=int, default=1,
                   help="Also compute for N days back (default: 1)")
    p.add_argument("--version",    type=int, default=1, help="Feature version tag")
    args = p.parse_args()

    # Load .env if present
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    session = get_session()
    try:
        pivot = (
            date.fromisoformat(args.as_of_date) if args.as_of_date
            else date.today()
        )
        for i in range(args.days_back + 1):
            run_pipeline(session, pivot - timedelta(days=i), args.version)
    finally:
        session.close()


if __name__ == "__main__":
    main()
