#!/usr/bin/env python3
"""
vertiv-data-platform/python/snowpark/dq_framework.py

DAMA 6-Dimension Data Quality Framework using Snowpark
Runs checks on Silver layer, logs results to PLATFORM_AUDIT.DQ tables

Usage:
  python dq_framework.py --source SAP
  python dq_framework.py --source ALL --batch-id my-batch-001
"""

import os
import sys
import uuid
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dq")

try:
    from snowflake.snowpark import Session
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.types import FloatType
except ImportError:
    print("Run: pip install -r requirements.txt")
    sys.exit(1)


# ── Snowflake session ─────────────────────────────────────────
def get_session() -> Session:
    for var in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD"]:
        if not os.environ.get(var):
            log.error(f"Missing env var: {var}.  Set in .env and run: source .env")
            sys.exit(1)
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": "INGEST_WH",
        "database":  "ENTERPRISE_CURATED",
        "schema":    "SALES",
        "role":      "PLATFORM_ADMIN",
    }).create()


# ── Result helper ─────────────────────────────────────────────
def make_result(batch_id, src, region, table, dim, rule,
                checked, passed, failed, quarantined, pass_rate):
    return {
        "batch_id": batch_id, "source_system": src, "source_region": region,
        "target_table": table, "dama_dimension": dim, "rule_name": rule,
        "records_checked": checked, "records_passed": passed,
        "records_failed": failed, "records_quarantined": quarantined,
        "pass_rate": round(pass_rate, 2),
    }


class DQFramework:
    """DAMA 6-Dimension DQ checks via Snowpark DataFrames."""

    def __init__(self, session: Session, batch_id: Optional[str] = None):
        self.session   = session
        self.batch_id  = batch_id or str(uuid.uuid4())
        self._results  = []
        log.info(f"DQFramework  batch_id={self.batch_id}")

    # ── 1. COMPLETENESS ──────────────────────────────────────
    def check_completeness(self, df, table: str, source: str,
                           region: str, mandatory: list) -> list:
        """All mandatory columns must be non-NULL."""
        total   = df.count()
        results = []
        log.info(f"  [COMPLETENESS] {table} — {total:,} rows")

        for col in mandatory:
            nulls    = df.filter(F.col(col).isNull()).count()
            passed   = total - nulls
            rate     = (passed / total * 100) if total else 0
            icon     = "✅" if rate >= 99 else "⚠️ " if rate >= 95 else "❌"
            log.info(f"    {icon} {col}: {rate:.1f}% ({nulls:,} nulls)")

            results.append(make_result(
                self.batch_id, source, region, table,
                "COMPLETENESS", f"{col}_NOT_NULL",
                total, passed, nulls,
                nulls if rate < 99 else 0, rate,
            ))

        self._flush(results)
        return results

    # ── 2. UNIQUENESS ────────────────────────────────────────
    def check_uniqueness(self, df, table: str, source: str,
                         region: str, pk_cols: list) -> list:
        """No duplicate primary key values."""
        total  = df.count()
        log.info(f"  [UNIQUENESS] {table}")

        dups   = df.group_by(pk_cols).count().filter(F.col("count") > 1).count()
        passed = total - dups
        rate   = (passed / total * 100) if total else 100
        icon   = "✅" if dups == 0 else "❌"
        log.info(f"    {icon} PK({', '.join(pk_cols)}): {rate:.1f}% ({dups:,} dupes)")

        r = [make_result(self.batch_id, source, region, table,
                         "UNIQUENESS", f"PK_UNIQUE({'_'.join(pk_cols)})",
                         total, passed, dups, dups, rate)]
        self._flush(r)
        return r

    # ── 3. VALIDITY ──────────────────────────────────────────
    def check_validity(self, df, table: str, source: str, region: str) -> list:
        """Domain rules: currency codes, date ranges, positive amounts."""
        total   = df.count()
        results = []
        log.info(f"  [VALIDITY] {table}")

        VALID_CURRENCIES = ["USD","EUR","GBP","CHF","CAD","AUD","JPY",
                            "CNY","INR","SGD","BRL","MXN","AED","SAR","ZAR","SEK","DKK"]

        if "CURRENCY_CODE" in df.columns:
            inv  = df.filter(~F.col("CURRENCY_CODE").isin(VALID_CURRENCIES)).count()
            rate = ((total - inv) / total * 100) if total else 100
            log.info(f"    {'✅' if rate >= 99 else '❌'} Currency ISO4217: {rate:.1f}% ({inv:,} invalid)")
            results.append(make_result(self.batch_id, source, region, table,
                "VALIDITY","CURRENCY_ISO4217",total,total-inv,inv,inv,rate))

        if "ORDER_DATE" in df.columns:
            bad  = df.filter(
                (F.col("ORDER_DATE") < F.lit("2010-01-01")) |
                (F.col("ORDER_DATE") > F.current_date())
            ).count()
            rate = ((total - bad) / total * 100) if total else 100
            log.info(f"    {'✅' if rate >= 99 else '❌'} Order date range: {rate:.1f}% ({bad:,} invalid)")
            results.append(make_result(self.batch_id, source, region, table,
                "VALIDITY","ORDER_DATE_RANGE",total,total-bad,bad,bad,rate))

        if "NET_AMOUNT_USD" in df.columns:
            neg  = df.filter(F.col("NET_AMOUNT_USD") < F.lit(0)).count()
            rate = ((total - neg) / total * 100) if total else 100
            log.info(f"    {'✅' if neg == 0 else '⚠️ '} Non-negative amount: {rate:.1f}% ({neg:,} negative)")
            results.append(make_result(self.batch_id, source, region, table,
                "VALIDITY","AMOUNT_NON_NEGATIVE",total,total-neg,neg,0,rate))

        self._flush(results)
        return results

    # ── 4. ACCURACY ──────────────────────────────────────────
    def check_accuracy(self, df, table: str, source: str, region: str) -> list:
        """Statistical outlier detection using 3σ rule."""
        total   = df.count()
        results = []
        log.info(f"  [ACCURACY] {table}")

        if "NET_AMOUNT_USD" in df.columns and total > 30:
            stats = df.select(
                F.mean("NET_AMOUNT_USD").alias("mu"),
                F.stddev("NET_AMOUNT_USD").alias("sigma"),
            ).collect()[0]
            mu, sigma = float(stats["MU"] or 0), float(stats["SIGMA"] or 1)
            outliers  = df.filter(
                F.abs(F.col("NET_AMOUNT_USD") - F.lit(mu)) > F.lit(3 * sigma)
            ).count()
            rate = ((total - outliers) / total * 100) if total else 100
            log.info(f"    {'✅' if rate >= 97 else '⚠️ '} Amount 3σ: {rate:.1f}% "
                     f"({outliers:,} outliers | μ=${mu:,.0f} σ=${sigma:,.0f})")
            results.append(make_result(self.batch_id, source, region, table,
                "ACCURACY","AMOUNT_3SIGMA_OUTLIER",total,total-outliers,outliers,0,rate))

        self._flush(results)
        return results

    # ── 5. TIMELINESS ────────────────────────────────────────
    def check_timeliness(self, df, table: str, source: str, region: str,
                         max_lag_hours: float = 4.0) -> list:
        """Records must arrive within SLA lag from order date."""
        total = df.count()
        log.info(f"  [TIMELINESS] {table}  (SLA={max_lag_hours}h)")

        if "LOAD_TIMESTAMP" not in df.columns or "ORDER_DATE" not in df.columns:
            log.warning("    ⚠️  Missing LOAD_TIMESTAMP or ORDER_DATE — skipped")
            return []

        late = df.filter(
            F.datediff("hour",
                       F.col("ORDER_DATE").cast("TIMESTAMP_NTZ"),
                       F.col("LOAD_TIMESTAMP")) > F.lit(max_lag_hours)
        ).count()
        rate = ((total - late) / total * 100) if total else 100
        log.info(f"    {'✅' if rate >= 95 else '⚠️ '} Load lag <{max_lag_hours}h: {rate:.1f}% ({late:,} late)")

        r = [make_result(self.batch_id, source, region, table,
                         "TIMELINESS", "LOAD_LAG_CHECK",
                         total, total-late, late, 0, rate)]
        self._flush(r)
        return r

    # ── 6. CONSISTENCY ───────────────────────────────────────
    def check_consistency(self, table: str, source: str, region: str) -> list:
        """Cross-source check: EUR orders should come from EUROPE region."""
        log.info(f"  [CONSISTENCY] {table}")
        try:
            df  = self.session.table(f"ENTERPRISE_CURATED.SALES.SALES_ORDER").filter(
                F.col("SOURCE_SYSTEM") == source
            )
            total = df.count()
            mismatches = df.filter(
                (F.col("SOURCE_REGION") == F.lit("EUROPE")) &
                (~F.col("CURRENCY_CODE").isin(["EUR","GBP","CHF","SEK","DKK"]))
            ).count()
            rate  = ((total - mismatches) / total * 100) if total else 100
            icon  = "✅" if rate >= 98 else "⚠️ "
            log.info(f"    {icon} EUR-region currency: {rate:.1f}% ({mismatches:,} mismatches)")
            r = [make_result(self.batch_id, source, region, table,
                             "CONSISTENCY","EUR_REGION_CURRENCY_MATCH",
                             total, total-mismatches, mismatches, 0, rate)]
            self._flush(r)
            return r
        except Exception as e:
            log.warning(f"    Consistency check skipped: {e}")
            return []

    # ── Flush results to audit DB ─────────────────────────────
    def _flush(self, results: list):
        if not results:
            return
        rows = [(
            r["batch_id"], r["source_system"], r.get("source_region",""),
            r["target_table"], r["dama_dimension"], r["rule_name"],
            int(r["records_checked"]), int(r["records_passed"]),
            int(r["records_failed"]), int(r["records_quarantined"]),
            float(r["pass_rate"]),
        ) for r in results]
        cur = self.session.connection.cursor()
        try:
            cur.executemany("""
                INSERT INTO PLATFORM_AUDIT.DQ.DQ_BATCH_LOG
                  (BATCH_ID, SOURCE_SYSTEM, SOURCE_REGION, TARGET_TABLE,
                   DAMA_DIMENSION, RULE_NAME, RECORDS_CHECKED, RECORDS_PASSED,
                   RECORDS_FAILED, RECORDS_QUARANTINED, PASS_RATE)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
        except Exception as e:
            log.warning(f"Audit flush failed (table may not exist yet): {e}")
        finally:
            cur.close()
        self._results.extend(results)

    # ── Run all 6 dimensions ──────────────────────────────────
    def run_all(self, source: str) -> dict:
        region_map = {"SAP":"EUROPE","JDE":"AMERICAS","QAD":"EMEA","SALESFORCE":"GLOBAL"}
        region     = region_map.get(source, "GLOBAL")

        log.info(f"\n{'='*55}")
        log.info(f"DAMA 6-Dimension DQ  source={source}  batch={self.batch_id}")
        log.info(f"{'='*55}")

        df = self.session.table("ENTERPRISE_CURATED.SALES.DT_SILVER_ORDER_SAP").filter(
            F.col("SOURCE_SYSTEM") == source
        )
        n = df.count()
        if n == 0:
            log.warning(f"No Silver data for source={source} — run data generator first")
            return {}

        report = {}
        report["completeness"] = self.check_completeness(
            df, "SALES_ORDER", source, region,
            ["ORDER_HK","ORIGINAL_ID","CUSTOMER_HK","ORDER_DATE","NET_AMOUNT_USD"]
        )
        report["uniqueness"]  = self.check_uniqueness(
            df, "SALES_ORDER", source, region, ["ORDER_HK","EFFECTIVE_FROM"]
        )
        report["validity"]    = self.check_validity(df, "SALES_ORDER", source, region)
        report["accuracy"]    = self.check_accuracy(df, "SALES_ORDER", source, region)
        report["timeliness"]  = self.check_timeliness(df, "SALES_ORDER", source, region)
        report["consistency"] = self.check_consistency("SALES_ORDER", source, region)

        # Summary
        all_results = [r for v in report.values() for r in v]
        avg_pass = sum(r["pass_rate"] for r in all_results) / len(all_results) if all_results else 0
        log.info(f"\n  Summary: {len(all_results)} checks | avg pass rate {avg_pass:.1f}%")
        log.info(f"  Batch logged to PLATFORM_AUDIT.DQ.DQ_BATCH_LOG")
        return report


def main():
    import argparse
    p = argparse.ArgumentParser(description="Enterprise Co DAMA 6-Dimension DQ Framework")
    p.add_argument("--source",   default="SAP",
                   choices=["SAP","JDE","QAD","SALESFORCE","ALL"])
    p.add_argument("--batch-id", default=None)
    args = p.parse_args()

    # Load .env if present
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    session = get_session()
    fw      = DQFramework(session, batch_id=args.batch_id)

    sources = ["SAP","JDE","QAD","SALESFORCE"] if args.source == "ALL" else [args.source]
    for src in sources:
        fw.run_all(src)

    session.close()
    log.info("\nDQ framework complete.")


# Allow import from feature_engineering.py without auto-running main()
from pathlib import Path
if __name__ == "__main__":
    main()
