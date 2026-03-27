"""
Data Quality Monitor + Root Cause Analysis Agent
=================================================
A production-quality Claude agent that monitors all active data products
in the Enterprise Data Marketplace, runs DAMA 6-dimension DQ checks, traces
failing rows back through Bronze→Silver→Gold lineage, generates a concise
RCA report, and fires alerts + GitHub issues for engineering follow-up.

Model : claude-sonnet-4-6
SDK   : anthropic (Tool Use / tool_use content blocks)
Usage : python dq_rca_agent.py
        (set SNOWFLAKE_ACCOUNT env var for live Snowflake; runs on mock data otherwise)
"""

import json
import logging
import os
from datetime import datetime
from typing import Any

import anthropic

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dq_rca_agent")


# ---------------------------------------------------------------------------
# Mock data (used when SNOWFLAKE_ACCOUNT is not set)
# ---------------------------------------------------------------------------
MOCK_PRODUCTS = [
    {
        "PRODUCT_ID": "DP-001",
        "PRODUCT_NAME": "Customer 360 View",
        "DOMAIN": "CUSTOMER_EXPERIENCE",
        "OWNER_TEAM": "cx-analytics@enterprise.com",
        "TRUST_SCORE": 99.2,
        "STATUS": "ACTIVE",
    },
    {
        "PRODUCT_ID": "DP-002",
        "PRODUCT_NAME": "Sales Performance Dashboard",
        "DOMAIN": "SALES_PERFORMANCE",
        "OWNER_TEAM": "sales-ops@enterprise.com",
        "TRUST_SCORE": 87.5,
        "STATUS": "ACTIVE",
    },
]

MOCK_DQ_SCORES = {
    "DP-001": {
        "PRODUCT_ID": "DP-001",
        "COMPLETENESS": 99.8,
        "UNIQUENESS": 100.0,
        "VALIDITY": 99.5,
        "ACCURACY": 98.9,
        "CONSISTENCY": 99.1,
        "TIMELINESS": 99.6,
        "OVERALL_TRUST_SCORE": 99.2,
    },
    "DP-002": {
        "PRODUCT_ID": "DP-002",
        "COMPLETENESS": 82.1,   # CURRENCY_CODE is null in 312 rows
        "UNIQUENESS": 99.7,
        "VALIDITY": 88.4,
        "ACCURACY": 90.0,
        "CONSISTENCY": 85.3,
        "TIMELINESS": 99.0,
        "OVERALL_TRUST_SCORE": 87.5,
        "FAILING_DIMENSION": "COMPLETENESS",
        "FAILING_COLUMN": "CURRENCY_CODE",
    },
}

MOCK_QUARANTINED_ROWS = {
    "DP-002": [
        {
            "ROW_ID": f"ROW-{1000 + i}",
            "BATCH_ID": "BATCH-2026-03-26-1400",
            "SOURCE_SYSTEM": "JDE_AMERICAS",
            "IS_QUARANTINED": True,
            "QUARANTINE_REASON": "CURRENCY_CODE IS NULL",
            "LOAD_DATE": "2026-03-26",
            "ORDER_ID": f"ORD-{50000 + i}",
        }
        for i in range(5)   # return first 5 as sample
    ]
}

MOCK_LINEAGE = {
    "BATCH-2026-03-26-1400": {
        "BATCH_ID": "BATCH-2026-03-26-1400",
        "SOURCE_SYSTEM": "JDE_AMERICAS",
        "SOURCE_TABLE": "RAW_JDE_ORDERS",
        "BRONZE_TABLE": "ENTERPRISE_RAW.BRONZE.RAW_JDE_SALES_ORDERS",
        "SILVER_TABLE": "ENTERPRISE_CURATED.SILVER.DT_SILVER_ORDER_JDE",
        "GOLD_TABLE": "ENTERPRISE_ANALYTICS.GOLD.FACT_SALES_ORDERS",
        "LOAD_TIMESTAMP": "2026-03-26T14:00:12Z",
        "ROW_COUNT_BRONZE": 8420,
        "ROW_COUNT_SILVER": 8108,
        "ROW_COUNT_GOLD": 8108,
        "QUARANTINED_COUNT": 312,
        "ROOT_CAUSE_HINT": (
            "JDE Americas batch loaded 2026-03-26 14:00 UTC is missing "
            "CURRENCY_CODE for 312 orders originating from subsidiary "
            "ENTERPRISE-MX (Mexico). Likely cause: JDE field mapping "
            "CRCD→CURRENCY_CODE absent in latest JDE connector version 4.2.1."
        ),
    }
}


# ---------------------------------------------------------------------------
# Tool Definitions (Anthropic JSON-schema format)
# ---------------------------------------------------------------------------
TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "run_snowflake_query",
        "description": (
            "Execute a SQL query against the Enterprise Co Snowflake environment. "
            "Returns a list of row dicts. Use for ad-hoc queries not covered "
            "by the other specialised tools."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL query to execute (SELECT only).",
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_marketplace_products",
        "description": (
            "Return all ACTIVE data products registered in the Enterprise Co Data "
            "Marketplace (V_DATA_MARKETPLACE view). Includes PRODUCT_ID, "
            "PRODUCT_NAME, DOMAIN, OWNER_TEAM, TRUST_SCORE, and STATUS."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_dq_scores",
        "description": (
            "Retrieve the latest DAMA 6-dimension data quality scores for a "
            "specific data product: Completeness, Uniqueness, Validity, "
            "Accuracy, Consistency, Timeliness, and overall TRUST_SCORE."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {
                    "type": "string",
                    "description": "The unique data product identifier (e.g. 'DP-002').",
                }
            },
            "required": ["product_id"],
        },
    },
    {
        "name": "get_quarantined_rows",
        "description": (
            "Find rows where IS_QUARANTINED = TRUE for a given data product. "
            "Returns sample rows with BATCH_ID, SOURCE_SYSTEM, and "
            "QUARANTINE_REASON to enable lineage tracing."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {
                    "type": "string",
                    "description": "The data product ID to check.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of quarantined rows to return (default 10).",
                    "default": 10,
                },
            },
            "required": ["product_id"],
        },
    },
    {
        "name": "trace_batch_lineage",
        "description": (
            "Trace a bad batch back through the Medallion lineage: "
            "Gold → Silver → Bronze → source system. Returns row counts "
            "at each layer, load timestamp, and a root-cause hint if available."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "batch_id": {
                    "type": "string",
                    "description": "The BATCH_ID to trace (e.g. 'BATCH-2026-03-26-1400').",
                },
                "source_system": {
                    "type": "string",
                    "description": "The source system (e.g. 'JDE_AMERICAS', 'SAP_ECC').",
                },
            },
            "required": ["batch_id", "source_system"],
        },
    },
    {
        "name": "send_alert",
        "description": (
            "Send a DQ alert to the domain owner team via Slack / email. "
            "Logs the alert and returns a confirmation dict with timestamp."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "owner_team": {
                    "type": "string",
                    "description": "Email or Slack handle of the owner team.",
                },
                "product_name": {
                    "type": "string",
                    "description": "Human-readable data product name.",
                },
                "rca_summary": {
                    "type": "string",
                    "description": "Concise 3-5 bullet RCA summary to include in the alert.",
                },
                "severity": {
                    "type": "string",
                    "enum": ["HIGH", "MEDIUM", "LOW"],
                    "description": "Alert severity level.",
                },
            },
            "required": ["owner_team", "product_name", "rca_summary", "severity"],
        },
    },
    {
        "name": "create_github_issue",
        "description": (
            "Create a GitHub issue in the modern-data-platform repository "
            "with the full RCA report for engineering follow-up. "
            "Returns the simulated issue URL and issue number."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Issue title (concise, < 80 chars).",
                },
                "body": {
                    "type": "string",
                    "description": "Full markdown body with RCA details, batch IDs, row counts.",
                },
                "labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "GitHub labels to apply (e.g. ['data-quality', 'bug', 'HIGH']).",
                },
            },
            "required": ["title", "body", "labels"],
        },
    },
]


# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a Data Quality Monitor Agent for the Modern AI Data Platform.
Your job is to:
1. Check all active data products in the marketplace for DQ issues (TRUST_SCORE < 95)
2. For any failing products, run DAMA 6-dimension checks (Completeness, Uniqueness, Validity, Accuracy, Consistency, Timeliness)
3. Trace failing rows back through the lineage: Gold → Silver → Bronze → source batch
4. Identify the root cause (bad batch, schema change, source system issue, transformation bug)
5. Generate a concise RCA report (3-5 bullet points, specific batch IDs and row counts)
6. Send an alert to the domain owner team with severity (HIGH/MEDIUM/LOW)
7. Create a GitHub issue with the full RCA for engineering follow-up

Always be specific — include batch IDs, row counts, column names, and source systems.
Never alert for TRUST_SCORE >= 98 (noise threshold).
"""


# ---------------------------------------------------------------------------
# Agent Class
# ---------------------------------------------------------------------------
class DQRCAAgent:
    """
    Data Quality Root Cause Analysis Agent.

    Uses Claude claude-sonnet-4-6 with tool use to autonomously monitor data
    products, identify DQ failures, trace lineage, and escalate issues.
    """

    def __init__(self) -> None:
        """Initialise Anthropic client and Snowflake connection parameters."""
        self.client = anthropic.Anthropic(
            api_key=os.environ.get("ANTHROPIC_API_KEY")
        )
        self.model = "claude-sonnet-4-6"
        self.max_tokens = 4096

        # Snowflake connection params (from environment)
        self.sf_account  = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        self.sf_user     = os.environ.get("SNOWFLAKE_USER", "")
        self.sf_password = os.environ.get("SNOWFLAKE_PASSWORD", "")
        self.sf_database = os.environ.get("SNOWFLAKE_DATABASE", "ENTERPRISE_ANALYTICS")
        self.sf_schema   = os.environ.get("SNOWFLAKE_SCHEMA", "GOLD")
        self.sf_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ENTERPRISE_WH")
        self.sf_role     = os.environ.get("SNOWFLAKE_ROLE", "ENTERPRISE_ANALYST")

        self._use_mock = not bool(self.sf_account)
        if self._use_mock:
            logger.info(
                "SNOWFLAKE_ACCOUNT not set — running in MOCK DATA mode. "
                "Set env vars to connect to live Snowflake."
            )
        else:
            logger.info("Snowflake account: %s  user: %s", self.sf_account, self.sf_user)

    # ------------------------------------------------------------------
    # Snowflake execution (with mock fallback)
    # ------------------------------------------------------------------
    def _run_snowflake_query(self, sql: str) -> list[dict]:
        """
        Execute a SQL query against Snowflake.

        Falls back to mock data if SNOWFLAKE_ACCOUNT is not configured,
        so the agent can run standalone for demos and unit tests.
        """
        if self._use_mock:
            logger.info("[MOCK] SQL: %s", sql.strip()[:120])
            return [{"mock": True, "note": "Snowflake not connected — mock mode active"}]

        try:
            import snowflake.connector  # type: ignore
            conn = snowflake.connector.connect(
                account=self.sf_account,
                user=self.sf_user,
                password=self.sf_password,
                database=self.sf_database,
                schema=self.sf_schema,
                warehouse=self.sf_warehouse,
                role=self.sf_role,
            )
            cur = conn.cursor(snowflake.connector.DictCursor)
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.error("Snowflake query failed: %s", exc)
            return [{"error": str(exc)}]

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------
    def _tool_run_snowflake_query(self, sql: str) -> list[dict]:
        return self._run_snowflake_query(sql)

    def _tool_get_marketplace_products(self) -> list[dict]:
        """Return active products — use mock data when offline."""
        if self._use_mock:
            logger.info("[MOCK] get_marketplace_products → %d products", len(MOCK_PRODUCTS))
            return MOCK_PRODUCTS
        sql = """
            SELECT PRODUCT_ID, PRODUCT_NAME, DOMAIN, OWNER_TEAM,
                   TRUST_SCORE, STATUS
            FROM   DATA_PRODUCTS.PUBLIC.V_DATA_MARKETPLACE
            WHERE  STATUS = 'ACTIVE'
            ORDER BY TRUST_SCORE ASC
        """
        return self._run_snowflake_query(sql)

    def _tool_get_dq_scores(self, product_id: str) -> dict:
        """Fetch DAMA 6-dimension scores for a product."""
        if self._use_mock:
            logger.info("[MOCK] get_dq_scores(%s)", product_id)
            return MOCK_DQ_SCORES.get(product_id, {"error": "product not found"})
        sql = f"""
            SELECT PRODUCT_ID, COMPLETENESS, UNIQUENESS, VALIDITY,
                   ACCURACY, CONSISTENCY, TIMELINESS, OVERALL_TRUST_SCORE
            FROM   ENTERPRISE_ANALYTICS.GOLD.DQ_DIMENSION_SCORES
            WHERE  PRODUCT_ID = '{product_id}'
            ORDER BY SCORE_DATE DESC
            LIMIT 1
        """
        rows = self._run_snowflake_query(sql)
        return rows[0] if rows else {"error": "no DQ score found"}

    def _tool_get_quarantined_rows(
        self, product_id: str, limit: int = 10
    ) -> list[dict]:
        """Return sample quarantined rows for a product."""
        if self._use_mock:
            logger.info("[MOCK] get_quarantined_rows(%s, limit=%d)", product_id, limit)
            rows = MOCK_QUARANTINED_ROWS.get(product_id, [])
            return rows[:limit]
        sql = f"""
            SELECT ROW_ID, BATCH_ID, SOURCE_SYSTEM,
                   IS_QUARANTINED, QUARANTINE_REASON, LOAD_DATE, ORDER_ID
            FROM   ENTERPRISE_ANALYTICS.GOLD.FACT_SALES_ORDERS
            WHERE  IS_QUARANTINED = TRUE
              AND  PRODUCT_ID     = '{product_id}'
            ORDER BY LOAD_DATE DESC
            LIMIT {limit}
        """
        return self._run_snowflake_query(sql)

    def _tool_trace_batch_lineage(
        self, batch_id: str, source_system: str
    ) -> dict:
        """Trace a batch through Bronze→Silver→Gold."""
        if self._use_mock:
            logger.info("[MOCK] trace_batch_lineage(%s, %s)", batch_id, source_system)
            return MOCK_LINEAGE.get(
                batch_id,
                {"error": f"no lineage found for batch {batch_id}"},
            )
        sql = f"""
            SELECT
                b.BATCH_ID,
                b.SOURCE_SYSTEM,
                b.SOURCE_TABLE,
                b.BRONZE_TABLE,
                b.SILVER_TABLE,
                b.GOLD_TABLE,
                b.LOAD_TIMESTAMP,
                b.ROW_COUNT_BRONZE,
                b.ROW_COUNT_SILVER,
                b.ROW_COUNT_GOLD,
                b.QUARANTINED_COUNT,
                b.ROOT_CAUSE_HINT
            FROM  ENTERPRISE_RAW.AUDIT.BATCH_LINEAGE b
            WHERE b.BATCH_ID     = '{batch_id}'
              AND b.SOURCE_SYSTEM = '{source_system}'
            LIMIT 1
        """
        rows = self._run_snowflake_query(sql)
        return rows[0] if rows else {"error": "lineage not found"}

    def _tool_send_alert(
        self,
        owner_team: str,
        product_name: str,
        rca_summary: str,
        severity: str,
    ) -> dict:
        """Simulate sending a Slack / email alert."""
        timestamp = datetime.utcnow().isoformat() + "Z"
        msg = (
            f"\n{'='*70}\n"
            f"[ALERT SENT]  severity={severity}  ts={timestamp}\n"
            f"  To      : {owner_team}\n"
            f"  Product : {product_name}\n"
            f"  Summary :\n{rca_summary}\n"
            f"{'='*70}\n"
        )
        logger.warning(msg)
        print(msg)
        return {
            "status": "sent",
            "severity": severity,
            "recipient": owner_team,
            "product": product_name,
            "timestamp": timestamp,
        }

    def _tool_create_github_issue(
        self, title: str, body: str, labels: list[str]
    ) -> dict:
        """Simulate creating a GitHub issue."""
        import random
        issue_num = random.randint(200, 999)
        repo      = "your-org/modern-data-platform"
        url       = f"https://github.com/{repo}/issues/{issue_num}"
        msg = (
            f"\n{'='*70}\n"
            f"[GITHUB ISSUE CREATED]  #{issue_num}\n"
            f"  Repo   : {repo}\n"
            f"  Title  : {title}\n"
            f"  Labels : {', '.join(labels)}\n"
            f"  URL    : {url}\n"
            f"  Body preview:\n{body[:300]}...\n"
            f"{'='*70}\n"
        )
        logger.info(msg)
        print(msg)
        return {
            "issue_number": issue_num,
            "url": url,
            "title": title,
            "labels": labels,
            "status": "created",
        }

    # ------------------------------------------------------------------
    # Tool dispatcher
    # ------------------------------------------------------------------
    def _execute_tool(self, tool_name: str, tool_input: dict[str, Any]) -> str:
        """
        Dispatch a tool_use call from Claude to the appropriate implementation.
        Returns the result serialised as a JSON string.
        """
        logger.info("Executing tool: %s  input=%s", tool_name, json.dumps(tool_input)[:200])

        try:
            if tool_name == "run_snowflake_query":
                result = self._tool_run_snowflake_query(tool_input["sql"])

            elif tool_name == "get_marketplace_products":
                result = self._tool_get_marketplace_products()

            elif tool_name == "get_dq_scores":
                result = self._tool_get_dq_scores(tool_input["product_id"])

            elif tool_name == "get_quarantined_rows":
                result = self._tool_get_quarantined_rows(
                    product_id=tool_input["product_id"],
                    limit=tool_input.get("limit", 10),
                )

            elif tool_name == "trace_batch_lineage":
                result = self._tool_trace_batch_lineage(
                    batch_id=tool_input["batch_id"],
                    source_system=tool_input["source_system"],
                )

            elif tool_name == "send_alert":
                result = self._tool_send_alert(
                    owner_team=tool_input["owner_team"],
                    product_name=tool_input["product_name"],
                    rca_summary=tool_input["rca_summary"],
                    severity=tool_input["severity"],
                )

            elif tool_name == "create_github_issue":
                result = self._tool_create_github_issue(
                    title=tool_input["title"],
                    body=tool_input["body"],
                    labels=tool_input.get("labels", []),
                )

            else:
                result = {"error": f"Unknown tool: {tool_name}"}

        except Exception as exc:  # noqa: BLE001
            logger.error("Tool %s raised an exception: %s", tool_name, exc)
            result = {"error": str(exc)}

        return json.dumps(result, default=str, indent=2)

    # ------------------------------------------------------------------
    # Main agentic loop
    # ------------------------------------------------------------------
    def run(self, trigger: str = "scheduled") -> str:
        """
        Execute the DQ Monitor + RCA agentic loop.

        Sends an initial message to Claude, then loops handling tool_use
        content blocks until the model returns stop_reason == 'end_turn'.

        Args:
            trigger: A string describing what triggered this run
                     (e.g. 'scheduled_15min_check', 'event_dq_drop').

        Returns:
            The final text response from Claude as a string.
        """
        logger.info("Agent run started  trigger=%s", trigger)

        messages: list[dict] = [
            {
                "role": "user",
                "content": (
                    f"Trigger: {trigger}\n"
                    f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n"
                    "Please perform a full Data Quality monitoring cycle:\n"
                    "1. Get all active marketplace products\n"
                    "2. Identify any with TRUST_SCORE < 95\n"
                    "3. For failing products, retrieve DAMA dimension scores and "
                    "quarantined rows\n"
                    "4. Trace the worst batch through lineage to find the root cause\n"
                    "5. Generate a 3-5 bullet RCA report\n"
                    "6. Send an alert (severity based on TRUST_SCORE drop depth)\n"
                    "7. Create a GitHub issue with the full RCA\n"
                    "8. Provide a final summary of what you found and did."
                ),
            }
        ]

        loop_count  = 0
        max_loops   = 20       # guard against runaway loops
        final_text  = ""

        while loop_count < max_loops:
            loop_count += 1
            logger.info("Agent loop iteration %d", loop_count)

            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            logger.info(
                "Claude response  stop_reason=%s  content_blocks=%d",
                response.stop_reason,
                len(response.content),
            )

            # Collect any text from this turn
            for block in response.content:
                if block.type == "text":
                    final_text = block.text
                    logger.info("Claude text: %s", block.text[:200])

            # If Claude is done, exit the loop
            if response.stop_reason == "end_turn":
                logger.info("Agent reached end_turn after %d iterations.", loop_count)
                break

            # If Claude wants to use tools, process all tool_use blocks
            if response.stop_reason == "tool_use":
                # Append assistant's message (with tool_use blocks) to history
                messages.append({"role": "assistant", "content": response.content})

                # Build the tool_result message for all tool calls in this turn
                tool_results: list[dict] = []
                for block in response.content:
                    if block.type == "tool_use":
                        tool_output = self._execute_tool(block.name, block.input)
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": tool_output,
                            }
                        )

                messages.append({"role": "user", "content": tool_results})
            else:
                # Unexpected stop reason — break to avoid infinite loop
                logger.warning(
                    "Unexpected stop_reason '%s' — exiting loop.", response.stop_reason
                )
                # Still capture any assistant content
                messages.append({"role": "assistant", "content": response.content})
                break

        if loop_count >= max_loops:
            logger.error("Agent exceeded max_loops=%d — forcing exit.", max_loops)
            final_text += "\n\n[AGENT WARNING: max loop iterations reached]"

        logger.info("Agent run complete.")
        return final_text


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  Modern AI Data Platform — DQ Monitor + RCA Agent")
    print("  Model  : claude-sonnet-4-6")
    print(f"  Mode   : {'LIVE (Snowflake)' if os.environ.get('SNOWFLAKE_ACCOUNT') else 'MOCK DATA'}")
    print("=" * 70 + "\n")

    agent = DQRCAAgent()
    result = agent.run(trigger="scheduled_15min_check")

    print("\n" + "=" * 70)
    print("FINAL AGENT RESPONSE:")
    print("=" * 70)
    print(result)
    print("=" * 70 + "\n")
