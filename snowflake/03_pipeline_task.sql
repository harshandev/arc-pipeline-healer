-- Step 3: Snowflake Task — runs HEAL_PIPELINE on schedule
-- Replace the A2A URLs with the ones you get from ArcAI Studio

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMOP;
USE SCHEMA DBT_PIPELINE;

-- ── The Task ──────────────────────────────────────────────────────
CREATE OR REPLACE TASK PIPELINE_HEALER_TASK
    WAREHOUSE = DG_WH
    SCHEDULE  = 'USING CRON 0 * * * * UTC'   -- every hour; change to '0 6 * * *' for daily 6am
    COMMENT   = 'ArcAI Pipeline Healer — detects failed dbt models and heals via A2A agents'
AS
    CALL HEAL_PIPELINE(
        'http://localhost:8300/api/a2a/pipeline-inspector',  -- replace with real Inspector A2A URL
        'http://localhost:8300/api/a2a/pipeline-healer',     -- replace with real Healer A2A URL
        24
    );

-- Activate the task (tasks start SUSPENDED by default)
ALTER TASK PIPELINE_HEALER_TASK RESUME;

-- ── Verify setup ──────────────────────────────────────────────────
SHOW TASKS LIKE 'PIPELINE_HEALER_TASK';

-- ── Manual trigger (use this during demo) ─────────────────────────
-- EXECUTE TASK PIPELINE_HEALER_TASK;

-- ── Check task run history ────────────────────────────────────────
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'PIPELINE_HEALER_TASK'
-- ));
