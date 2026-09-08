-- ============================================================================
-- BRD Reference: BRD-AERO-2026-0908
-- Jira Issue Key: SCRUM-27
-- Description: DDL for AEROSPACE_PARTS_TARGET, ETL_CONTROL, ETL_RECONCILIATION_LOG
-- Database: gen_ai_poc_snowflakecoe | Schema: sdlc_wizard
-- ============================================================================

USE DATABASE gen_ai_poc_snowflakecoe;
USE SCHEMA sdlc_wizard;

CREATE TABLE IF NOT EXISTS gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET (
    PART_NUMBER             VARCHAR(100)    NOT NULL,
    PART_NAME               VARCHAR(500),
    MANUFACTURER            VARCHAR(500),
    CATEGORY                VARCHAR(200),
    SUBCATEGORY             VARCHAR(200),
    WEIGHT_KG               NUMBER(18,6),
    UNIT_PRICE_USD          NUMBER(18,2),
    CURRENCY                VARCHAR(10),
    LEAD_TIME_DAYS          NUMBER(10,0),
    CERTIFICATION_STATUS    VARCHAR(100),
    LIFECYCLE_STATUS        VARCHAR(100),
    INSTALLATION_DATE       DATE,
    LAST_INSPECTION_DATE    DATE,
    NEXT_INSPECTION_DATE    DATE,
    SUPPLIER_CODE           VARCHAR(100),
    WAREHOUSE_LOCATION      VARCHAR(200),
    QUANTITY_ON_HAND        NUMBER(18,0),
    REORDER_POINT           NUMBER(18,0),
    AIRCRAFT_MODEL          VARCHAR(200),
    COMPLIANCE_REGION       VARCHAR(200),
    RISK_SCORE              VARCHAR(20),
    STATUS                  VARCHAR(50)     DEFAULT 'Active',
    CREATED_ON              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_ON              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_AEROSPACE_PARTS_TARGET PRIMARY KEY (PART_NUMBER)
);

CREATE TABLE IF NOT EXISTS gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL (
    PIPELINE_NAME           VARCHAR(200)    NOT NULL,
    LAST_RUN_TIMESTAMP      TIMESTAMP_NTZ,
    LAST_STATUS             VARCHAR(50),
    UPDATED_BY              VARCHAR(200)    DEFAULT CURRENT_USER(),
    UPDATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ETL_CONTROL PRIMARY KEY (PIPELINE_NAME)
);

CREATE TABLE IF NOT EXISTS gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_RECONCILIATION_LOG (
    RUN_ID                  VARCHAR(36)     NOT NULL,
    PIPELINE_NAME           VARCHAR(200),
    RUN_TIMESTAMP           TIMESTAMP_NTZ,
    SOURCE_COUNT            NUMBER(18,0),
    TARGET_COUNT            NUMBER(18,0),
    INSERTED_COUNT          NUMBER(18,0),
    UPDATED_COUNT           NUMBER(18,0),
    SOFT_DELETED_COUNT      NUMBER(18,0),
    STATUS                  VARCHAR(50),
    DURATION_SECONDS        NUMBER(18,2),
    ERROR_MESSAGE           VARCHAR(4000),
    CREATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ETL_RECONCILIATION_LOG PRIMARY KEY (RUN_ID)
);

INSERT INTO gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL (PIPELINE_NAME, LAST_RUN_TIMESTAMP, LAST_STATUS)
SELECT 'SP_LOAD_AEROSPACE_PARTS_SCD1', NULL, NULL
WHERE NOT EXISTS (
    SELECT 1 FROM gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL
    WHERE PIPELINE_NAME = 'SP_LOAD_AEROSPACE_PARTS_SCD1'
);