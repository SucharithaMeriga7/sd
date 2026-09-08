-- ============================================================================
-- BRD Reference: BRD-AERO-2026-0908
-- Jira Issue Key: SCRUM-27
-- Description: Snowflake Task to schedule SP_LOAD_AEROSPACE_PARTS_SCD1 daily
--              at 02:00 UTC
-- Database: gen_ai_poc_snowflakecoe | Schema: sdlc_wizard
-- ============================================================================

CREATE OR REPLACE TASK gen_ai_poc_snowflakecoe.sdlc_wizard.TASK_LOAD_AEROSPACE_PARTS_SCD1
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'
    COMMENT = 'SCRUM-27 | BRD-AERO-2026-0908 | Daily SCD1 load for AEROSPACE_PARTS at 02:00 UTC'
AS
    CALL gen_ai_poc_snowflakecoe.sdlc_wizard.SP_LOAD_AEROSPACE_PARTS_SCD1();

ALTER TASK gen_ai_poc_snowflakecoe.sdlc_wizard.TASK_LOAD_AEROSPACE_PARTS_SCD1 RESUME;