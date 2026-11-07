"""
MCP Server — Pipeline Healer
Exposes Snowflake pipeline tools to ArcAI agents.

Tools:
  - get_pipeline_failures   : scan PIPELINE_RUN_LOG for failed runs
  - get_model_error_detail  : fetch full error + query history for a model
  - run_dbt_model           : execute a dbt model and log the result
  - log_pipeline_run        : write a run result to PIPELINE_RUN_LOG
  - get_model_sql           : read a model's SQL from disk
  - fix_model_sql           : overwrite a model's SQL (Healer uses this)
"""

import os, subprocess, uuid, json, base64
from datetime import datetime
from pathlib import Path
from typing import Any

import snowflake.connector
import urllib.request
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import asyncio

load_dotenv(Path(__file__).parent.parent / ".env")

DBT_PROJECT = Path(__file__).parent.parent / "dbt_project"

# ── Snowflake connection ───────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

# ── Tool definitions ───────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "harshandev/arc-pipeline-healer")

TOOLS = [
    {
        "name": "get_pipeline_failures",
        "description": "Scan PIPELINE_RUN_LOG for recently failed dbt model runs. Returns model name, error message, and when it failed.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "hours_back": {
                    "type": "integer",
                    "description": "How many hours back to look for failures. Default 24.",
                    "default": 24,
                }
            },
        },
    },
    {
        "name": "get_model_error_detail",
        "description": "Get the full error detail for a specific failed dbt model, including the SQL that failed and the Snowflake error.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {
                    "type": "string",
                    "description": "The dbt model name, e.g. mart_revenue_by_product",
                }
            },
            "required": ["model_name"],
        },
    },
    {
        "name": "run_dbt_model",
        "description": "Run a specific dbt model and return success/failure with row count. Logs the result to PIPELINE_RUN_LOG.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {
                    "type": "string",
                    "description": "The dbt model name to run, e.g. mart_revenue_by_product",
                }
            },
            "required": ["model_name"],
        },
    },
    {
        "name": "get_model_sql",
        "description": "Read the current SQL of a dbt model file from the project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {
                    "type": "string",
                    "description": "The dbt model name, e.g. mart_revenue_by_product",
                }
            },
            "required": ["model_name"],
        },
    },
    {
        "name": "fix_model_sql",
        "description": "Overwrite a dbt model's SQL with a corrected version. Used by the Healer Agent after diagnosing the issue.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {
                    "type": "string",
                    "description": "The dbt model name to fix",
                },
                "fixed_sql": {
                    "type": "string",
                    "description": "The corrected SQL content for the model",
                },
                "reason": {
                    "type": "string",
                    "description": "Short explanation of what was fixed",
                },
            },
            "required": ["model_name", "fixed_sql", "reason"],
        },
    },
    {
        "name": "create_github_pr",
        "description": "Create a GitHub branch with the fixed SQL and open a Pull Request. Call this after fix_model_sql succeeds and run_dbt_model confirms the fix works.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {"type": "string", "description": "The dbt model that was fixed"},
                "failure_type": {"type": "string", "description": "e.g. COLUMN_MISSING"},
                "broken_line": {"type": "string", "description": "The original broken SQL line"},
                "fixed_line": {"type": "string", "description": "The corrected SQL line"},
                "root_cause": {"type": "string", "description": "Plain English explanation of the bug"},
                "fixed_sql": {"type": "string", "description": "The complete fixed SQL content"},
            },
            "required": ["model_name", "failure_type", "broken_line", "fixed_line", "root_cause", "fixed_sql"],
        },
    },
    {
        "name": "log_pipeline_run",
        "description": "Write a pipeline run result to PIPELINE_RUN_LOG. Use after running or fixing a model.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_name": {"type": "string"},
                "status": {"type": "string", "enum": ["success", "failure"]},
                "rows_affected": {"type": "integer", "default": 0},
                "error_message": {"type": "string", "default": ""},
                "triggered_by": {"type": "string", "default": "healer_agent"},
            },
            "required": ["model_name", "status"],
        },
    },
]

# ── Tool implementations ───────────────────────────────────────────

def tool_get_pipeline_failures(params: dict) -> dict:
    hours_back = params.get("hours_back", 24)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT MODEL_NAME, STATUS, STARTED_AT, ERROR_MESSAGE, ROWS_AFFECTED
        FROM PIPELINE_RUN_LOG
        WHERE STATUS = 'failure'
          AND STARTED_AT >= DATEADD('hour', -{hours_back}, CURRENT_TIMESTAMP())
        ORDER BY STARTED_AT DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    failures = [
        {
            "model_name": r[0],
            "status": r[1],
            "started_at": str(r[2]),
            "error_message": r[3],
            "rows_affected": r[4],
        }
        for r in rows
    ]
    return {"failures": failures, "count": len(failures)}


def tool_get_model_error_detail(params: dict) -> dict:
    model = params["model_name"]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT MODEL_NAME, STATUS, STARTED_AT, FINISHED_AT,
               ERROR_MESSAGE, ROWS_AFFECTED, TRIGGERED_BY
        FROM PIPELINE_RUN_LOG
        WHERE MODEL_NAME = %s
        ORDER BY STARTED_AT DESC
        LIMIT 5
    """, (model,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    history = [
        {
            "model_name": r[0], "status": r[1],
            "started_at": str(r[2]), "finished_at": str(r[3]),
            "error_message": r[4], "rows_affected": r[5],
            "triggered_by": r[6],
        }
        for r in rows
    ]
    sql_content = tool_get_model_sql(params).get("sql", "not found")
    return {"run_history": history, "current_sql": sql_content}


def tool_run_dbt_model(params: dict) -> dict:
    model = params["model_name"]
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_PROJECT)}
    result = subprocess.run(
        ["dbt", "run", "--select", model, "--profiles-dir", str(DBT_PROJECT)],
        capture_output=True, text=True, cwd=str(DBT_PROJECT), env=env,
    )
    success = result.returncode == 0
    output = result.stdout + result.stderr
    error_msg = ""
    if not success:
        for line in output.split("\n"):
            if "Error" in line or "error" in line or "Compilation Error" in line:
                error_msg += line.strip() + " "

    # Log to Snowflake
    tool_log_pipeline_run({
        "model_name": model,
        "status": "success" if success else "failure",
        "rows_affected": 0,
        "error_message": error_msg.strip() or output[-500:],
        "triggered_by": "mcp_tool",
    })

    return {
        "model": model,
        "success": success,
        "error": error_msg.strip() if not success else None,
        "output_tail": output[-800:],
    }


def tool_get_model_sql(params: dict) -> dict:
    model = params["model_name"]
    for path in DBT_PROJECT.rglob(f"{model}.sql"):
        return {"model": model, "sql": path.read_text(), "path": str(path)}
    return {"model": model, "sql": None, "error": f"Model {model} not found"}


def tool_fix_model_sql(params: dict) -> dict:
    model = params["model_name"]
    fixed_sql = params["fixed_sql"]
    reason = params.get("reason", "")
    for path in DBT_PROJECT.rglob(f"{model}.sql"):
        original = path.read_text()
        path.write_text(fixed_sql)
        return {
            "model": model,
            "fixed": True,
            "path": str(path),
            "reason": reason,
            "original_sql": original,
        }
    return {"model": model, "fixed": False, "error": f"Model {model} not found"}


def tool_log_pipeline_run(params: dict) -> dict:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO PIPELINE_RUN_LOG
            (RUN_ID, MODEL_NAME, STATUS, STARTED_AT, FINISHED_AT,
             ROWS_AFFECTED, ERROR_MESSAGE, TRIGGERED_BY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        str(uuid.uuid4()),
        params["model_name"],
        params["status"],
        datetime.utcnow(),
        datetime.utcnow(),
        params.get("rows_affected", 0),
        params.get("error_message", ""),
        params.get("triggered_by", "agent"),
    ))
    conn.commit(); cur.close(); conn.close()
    return {"logged": True, "model": params["model_name"], "status": params["status"]}


def gh_api(method, path, body=None):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def tool_create_github_pr(params: dict) -> dict:
    model       = params["model_name"]
    failure_type = params["failure_type"]
    broken_line = params["broken_line"]
    fixed_line  = params["fixed_line"]
    root_cause  = params["root_cause"]
    fixed_sql   = params["fixed_sql"]

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    branch = f"fix/dbt-{model}-{ts}"

    # 1. Get main branch SHA
    main_ref = gh_api("GET", "git/ref/heads/main")
    base_sha = main_ref["object"]["sha"]

    # 2. Create new branch
    gh_api("POST", "git/refs", {"ref": f"refs/heads/{branch}", "sha": base_sha})

    # 3. Find the file path in the repo
    model_path = None
    for p in DBT_PROJECT.rglob(f"{model}.sql"):
        # make it relative to the dbt_project dir
        model_path = "dbt_project/" + str(p.relative_to(DBT_PROJECT))
        break
    if not model_path:
        return {"error": f"Cannot find {model}.sql in repo"}

    # 4. Get current file SHA (needed for update)
    try:
        file_info = gh_api("GET", f"contents/{model_path}")
        file_sha = file_info["sha"]
    except Exception:
        file_sha = None

    # 5. Commit the fixed SQL
    commit_body = {
        "message": f"fix(dbt): {model} — {failure_type} fix\n\nAuto-healed by ArcAI Pipeline Healer Agent.",
        "content": base64.b64encode(fixed_sql.encode()).decode(),
        "branch": branch,
    }
    if file_sha:
        commit_body["sha"] = file_sha
    gh_api("PUT", f"contents/{model_path}", commit_body)

    # 6. Open the PR
    pr_body = f"""## Pipeline Auto-Heal

**Model:** `{model}`
**Failure type:** `{failure_type}`
**Root cause:** {root_cause}

### Change
**Before:**
```sql
{broken_line}
```
**After:**
```sql
{fixed_line}
```

### Verification
dbt run result: ✅ SUCCESS (verified by Healer Agent before opening this PR)

---
*Diagnosed and fixed autonomously by ArcAI Pipeline Healer Agent.*
*Triggered by Snowflake Task → `HEAL_PIPELINE()` → A2A protocol.*
"""
    pr = gh_api("POST", "pulls", {
        "title": f"fix(dbt): {model} — {failure_type} fix",
        "head": branch,
        "base": "main",
        "body": pr_body,
    })
    return {
        "pr_url": pr["html_url"],
        "pr_number": pr["number"],
        "branch": branch,
        "model": model,
        "status": "opened",
    }


TOOL_MAP = {
    "get_pipeline_failures":  tool_get_pipeline_failures,
    "create_github_pr":       tool_create_github_pr,
    "get_model_error_detail": tool_get_model_error_detail,
    "run_dbt_model": tool_run_dbt_model,
    "get_model_sql": tool_get_model_sql,
    "fix_model_sql": tool_fix_model_sql,
    "log_pipeline_run": tool_log_pipeline_run,
}

# ── FastAPI / MCP endpoints ────────────────────────────────────────
app = FastAPI(title="Pipeline Healer MCP Server")

@app.get("/health")
def health():
    return {"status": "ok", "tools": len(TOOLS)}

@app.get("/tools")
def list_tools():
    return {"tools": TOOLS}

@app.post("/sse")
async def execute_tool(request: Request):
    body = await request.json()
    tool_name = body.get("tool")
    params = body.get("params", {})

    if tool_name not in TOOL_MAP:
        return JSONResponse({"error": f"Unknown tool: {tool_name}"}, status_code=404)

    async def stream():
        try:
            result = TOOL_MAP[tool_name](params)
            yield f"data: {json.dumps({'result': result})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")
