#!/usr/bin/env python3
"""
vertiv-data-platform/scripts/deploy.py

Executes all Snowflake SQL files in the correct order.
Run after setting SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env

Usage:
  python scripts/deploy.py                         # dry-run (safe default)
  python scripts/deploy.py --execute               # actually run SQL
  python scripts/deploy.py --execute --step 03     # run only silver layer
  python scripts/deploy.py --execute --from-step 04 # run gold onwards
"""

import os, sys, argparse, logging, time
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("deploy.log", mode="a"),
    ],
)
log = logging.getLogger("deploy")

# ── Ordered execution plan ─────────────────────────────────────
# (role, sql_file)
PLAN = [
    ("ACCOUNTADMIN",           "sql/00_setup/01_databases_warehouses.sql"),
    ("SECURITYADMIN",          "sql/00_setup/02_roles_rbac.sql"),
    ("VERTIV_PLATFORM_ADMIN",  "sql/01_config/01_config_tables.sql"),
    ("VERTIV_PLATFORM_ADMIN",  "sql/02_bronze/01_bronze_ddl.sql"),
    ("VERTIV_PLATFORM_ADMIN",  "sql/03_silver/01_silver_ddl.sql"),
    ("VERTIV_PLATFORM_ADMIN",  "sql/04_gold/01_gold_bi.sql"),
    ("VERTIV_PLATFORM_ADMIN",  "sql/04_gold/02_gold_ml_ai.sql"),
    ("ACCOUNTADMIN",           "sql/05_security/01_security_data_products.sql"),
]


def load_env():
    """Load .env from project root."""
    root = Path(__file__).parent.parent
    env  = root / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def get_conn(role: str = "ACCOUNTADMIN"):
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = role,
        warehouse = "INGEST_WH",
        login_timeout = 30,
    )


def split_sql(text: str) -> list:
    """Split on semicolons, skip blanks and comment-only blocks."""
    stmts = []
    for raw in text.split(";"):
        s = raw.strip()
        if not s:
            continue
        # Skip if every non-empty line is a comment
        lines = [l for l in s.splitlines() if l.strip()]
        if lines and all(l.strip().startswith("--") for l in lines):
            continue
        stmts.append(s)
    return stmts


def run_file(conn, sql_file: str, role: str, dry_run: bool) -> dict:
    root  = Path(__file__).parent.parent
    fpath = root / sql_file
    if not fpath.exists():
        return {"file": sql_file, "ok": False, "reason": "FILE NOT FOUND", "stmts": 0}

    content = fpath.read_text(encoding="utf-8")
    stmts   = split_sql(content)
    log.info(f"  {fpath.name}  ({len(stmts)} statements)  role={role}")

    if dry_run:
        log.info(f"    [DRY-RUN] would execute {len(stmts)} statements")
        return {"file": sql_file, "ok": True, "reason": "DRY-RUN", "stmts": len(stmts)}

    cur     = conn.cursor()
    ok_cnt  = err_cnt = 0
    errors  = []

    for i, stmt in enumerate(stmts, 1):
        try:
            cur.execute(f"USE ROLE {role}")
        except Exception:
            pass
        try:
            cur.execute(stmt)
            ok_cnt += 1
            # Show first row for SHOW / SELECT
            kw = stmt.strip().upper().split()[0] if stmt.strip() else ""
            if kw in ("SHOW","SELECT","DESCRIBE"):
                rows = cur.fetchmany(3)
                if rows:
                    log.debug(f"      → {rows[0]}")
        except Exception as e:
            msg = str(e)
            IGNORE = ["ALREADY EXISTS","OBJECT ALREADY EXISTS","IF NOT EXISTS",
                      "DUPLICATE","DOES NOT EXIST"]
            if any(x in msg.upper() for x in IGNORE):
                ok_cnt += 1   # idempotent — treat as ok
            else:
                err_cnt += 1
                errors.append(f"  stmt {i}: {msg[:200]}")
                log.warning(f"    [WARN] stmt {i}: {msg[:120]}")

    cur.close()
    status = "[OK]" if err_cnt == 0 else f"[WARN] {err_cnt} errors"
    log.info(f"    {status}  ({ok_cnt} ok)")
    if errors:
        for e in errors[:3]:
            log.info(e)
    return {
        "file": sql_file, "ok": err_cnt == 0,
        "reason": "; ".join(errors) if errors else "OK",
        "stmts": len(stmts),
    }


def deploy(execute: bool, step: str = None, from_step: str = None):
    load_env()

    # Validate env vars
    for var in ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD"]:
        if not os.environ.get(var):
            log.error(f"Missing: {var}  —  copy .env.template to .env and fill in values")
            sys.exit(1)

    # Filter plan
    plan = PLAN
    if step:
        plan = [(r, f) for r, f in PLAN if Path(f).parts[1].startswith(step)]
    elif from_step:
        plan = [(r, f) for r, f in PLAN if Path(f).parts[1] >= from_step]

    log.info(f"\n{'='*55}")
    log.info(f"Vertiv Snowflake Deploy  {'EXECUTE' if execute else 'DRY-RUN'}")
    log.info(f"Account : {os.environ['SNOWFLAKE_ACCOUNT']}")
    log.info(f"Steps   : {len(plan)}")
    log.info(f"Started : {datetime.now().isoformat()}")
    log.info(f"{'='*55}\n")

    if not execute:
        log.info("DRY-RUN MODE — no changes will be made to Snowflake")
        log.info("Pass --execute to actually run SQL\n")

    try:
        conn = get_conn("ACCOUNTADMIN")
        log.info("[OK] Connected to Snowflake\n")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        sys.exit(1)

    results = []
    t0      = time.time()
    for role, sql_file in plan:
        r = run_file(conn, sql_file, role, dry_run=not execute)
        results.append(r)
    conn.close()

    elapsed = time.time() - t0
    passed  = sum(1 for r in results if r["ok"])
    failed  = len(results) - passed

    log.info(f"\n{'='*55}")
    log.info("DEPLOYMENT SUMMARY")
    log.info(f"{'='*55}")
    for r in results:
        icon = "[OK]" if r["ok"] else "[ERROR]"
        log.info(f"  {icon}  {r['file']}")
        if not r["ok"]:
            log.info(f"       reason: {r['reason'][:100]}")

    log.info(f"\n  Total : {len(results)}  OK:{passed}  ERR:{failed}")
    log.info(f"  Time  : {elapsed:.1f}s")
    log.info(f"  Log   : deploy.log")

    if failed:
        log.error("\n[ERROR]  Deployment had errors — review deploy.log")
        sys.exit(1)
    else:
        mode = "executed" if execute else "dry-run passed"
        log.info(f"\n[DONE]  Deployment {mode}!")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Vertiv Snowflake deploy script")
    p.add_argument("--execute",    action="store_true",
                   help="Actually run SQL (default: dry-run)")
    p.add_argument("--step",       default=None,
                   help="Run only steps starting with this prefix, e.g. 03")
    p.add_argument("--from-step",  default=None,
                   help="Run from this step onwards, e.g. 04")
    args = p.parse_args()
    deploy(execute=args.execute, step=args.step, from_step=args.from_step)
