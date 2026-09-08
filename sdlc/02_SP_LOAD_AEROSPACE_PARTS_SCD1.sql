-- ============================================================================
-- BRD Reference: BRD-AERO-2026-0908
-- Jira Issue Key: SCRUM-27
-- Procedure: SP_LOAD_AEROSPACE_PARTS_SCD1
-- Description: Automated SCD Type 1 pipeline with incremental load,
--              deduplication, exclusion rules, transformations, soft delete,
--              and reconciliation logging.
-- Database: gen_ai_poc_snowflakecoe | Schema: sdlc_wizard
-- ============================================================================

CREATE OR REPLACE PROCEDURE gen_ai_poc_snowflakecoe.sdlc_wizard.SP_LOAD_AEROSPACE_PARTS_SCD1()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    V_RUN_ID VARCHAR;
    V_START_TS TIMESTAMP_NTZ;
    V_LAST_RUN TIMESTAMP_NTZ;
    V_SOURCE_COUNT NUMBER DEFAULT 0;
    V_TARGET_COUNT NUMBER DEFAULT 0;
    V_INSERTED_COUNT NUMBER DEFAULT 0;
    V_UPDATED_COUNT NUMBER DEFAULT 0;
    V_SOFT_DELETED_COUNT NUMBER DEFAULT 0;
    V_DURATION_SECONDS NUMBER DEFAULT 0;
    V_STATUS VARCHAR DEFAULT 'SUCCESS';
    V_ERROR_MSG VARCHAR DEFAULT NULL;
    V_SUMMARY VARCHAR;
    V_PRE_MERGE_TARGET_COUNT NUMBER DEFAULT 0;
    V_POST_MERGE_ACTIVE_COUNT NUMBER DEFAULT 0;

    V_RUN_ID := (SELECT UUID_STRING());
    V_START_TS := CURRENT_TIMESTAMP()::TIMESTAMP_NTZ;

    -- Fetch last successful run timestamp for incremental load
    SELECT LAST_RUN_TIMESTAMP INTO :V_LAST_RUN
    FROM gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL
    WHERE PIPELINE_NAME = 'SP_LOAD_AEROSPACE_PARTS_SCD1';

    -- Capture pre-merge target count to derive inserted vs updated
    SELECT COUNT(*) INTO :V_PRE_MERGE_TARGET_COUNT
    FROM gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET;

    -- Create temp table with transformed, deduplicated, filtered source data
    CREATE OR REPLACE TEMPORARY TABLE gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC AS
    WITH RAW_INCREMENTAL AS (
        SELECT *
        FROM gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_SOURCE
        WHERE (:V_LAST_RUN IS NULL OR UPDATED_ON > :V_LAST_RUN)
    ),
    DEDUPED AS (
        SELECT *
        FROM RAW_INCREMENTAL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PART_NUMBER ORDER BY UPDATED_ON DESC) = 1
    ),
    FILTERED AS (
        SELECT *
        FROM DEDUPED
        WHERE NOT (
            LIFECYCLE_STATUS = 'End of Life'
            AND INSTALLATION_DATE < DATEADD(YEAR, -3, CURRENT_DATE())
        )
    ),
    TRANSFORMED AS (
        SELECT
            PART_NUMBER,
            PART_NAME,
            INITCAP(MANUFACTURER) AS MANUFACTURER,
            CATEGORY,
            SUBCATEGORY,
            IFF(WEIGHT_KG <= 0, NULL, WEIGHT_KG) AS WEIGHT_KG,
            ROUND(UNIT_PRICE_USD, 2) AS UNIT_PRICE_USD,
            CURRENCY,
            LEAD_TIME_DAYS,
            CERTIFICATION_STATUS,
            LIFECYCLE_STATUS,
            INSTALLATION_DATE,
            LAST_INSPECTION_DATE,
            NEXT_INSPECTION_DATE,
            SUPPLIER_CODE,
            WAREHOUSE_LOCATION,
            QUANTITY_ON_HAND,
            REORDER_POINT,
            AIRCRAFT_MODEL,
            COMPLIANCE_REGION,
            CASE
                WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 90 THEN 'High Risk'
                WHEN CERTIFICATION_STATUS IN ('FAA','EASA','Dual') AND LEAD_TIME_DAYS <= 60 THEN 'Low Risk'
                WHEN CERTIFICATION_STATUS = 'Pending' OR LEAD_TIME_DAYS > 120 THEN 'Medium Risk'
                ELSE 'Medium Risk'
            END AS RISK_SCORE,
            UPDATED_ON
        FROM FILTERED
    )
    SELECT * FROM TRANSFORMED;

    -- Source count from the prepared temp table
    SELECT COUNT(*) INTO :V_SOURCE_COUNT
    FROM gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC;

    -- SCD Type 1 MERGE
    MERGE INTO gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET AS TGT
    USING gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC AS SRC
        ON TGT.PART_NUMBER = SRC.PART_NUMBER
    WHEN MATCHED THEN UPDATE
        SET TGT.PART_NAME = SRC.PART_NAME,
            TGT.MANUFACTURER = SRC.MANUFACTURER,
            TGT.CATEGORY = SRC.CATEGORY,
            TGT.SUBCATEGORY = SRC.SUBCATEGORY,
            TGT.WEIGHT_KG = SRC.WEIGHT_KG,
            TGT.UNIT_PRICE_USD = SRC.UNIT_PRICE_USD,
            TGT.CURRENCY = SRC.CURRENCY,
            TGT.LEAD_TIME_DAYS = SRC.LEAD_TIME_DAYS,
            TGT.CERTIFICATION_STATUS = SRC.CERTIFICATION_STATUS,
            TGT.LIFECYCLE_STATUS = SRC.LIFECYCLE_STATUS,
            TGT.INSTALLATION_DATE = SRC.INSTALLATION_DATE,
            TGT.LAST_INSPECTION_DATE = SRC.LAST_INSPECTION_DATE,
            TGT.NEXT_INSPECTION_DATE = SRC.NEXT_INSPECTION_DATE,
            TGT.SUPPLIER_CODE = SRC.SUPPLIER_CODE,
            TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
            TGT.QUANTITY_ON_HAND = SRC.QUANTITY_ON_HAND,
            TGT.REORDER_POINT = SRC.REORDER_POINT,
            TGT.AIRCRAFT_MODEL = SRC.AIRCRAFT_MODEL,
            TGT.COMPLIANCE_REGION = SRC.COMPLIANCE_REGION,
            TGT.RISK_SCORE = SRC.RISK_SCORE,
            TGT.STATUS = 'Active',
            TGT.UPDATED_ON = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
            PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY, SUBCATEGORY,
            WEIGHT_KG, UNIT_PRICE_USD, CURRENCY, LEAD_TIME_DAYS,
            CERTIFICATION_STATUS, LIFECYCLE_STATUS, INSTALLATION_DATE,
            LAST_INSPECTION_DATE, NEXT_INSPECTION_DATE, SUPPLIER_CODE,
            WAREHOUSE_LOCATION, QUANTITY_ON_HAND, REORDER_POINT,
            AIRCRAFT_MODEL, COMPLIANCE_REGION, RISK_SCORE, STATUS,
            CREATED_ON, UPDATED_ON
        ) VALUES (
            SRC.PART_NUMBER, SRC.PART_NAME, SRC.MANUFACTURER, SRC.CATEGORY, SRC.SUBCATEGORY,
            SRC.WEIGHT_KG, SRC.UNIT_PRICE_USD, SRC.CURRENCY, SRC.LEAD_TIME_DAYS,
            SRC.CERTIFICATION_STATUS, SRC.LIFECYCLE_STATUS, SRC.INSTALLATION_DATE,
            SRC.LAST_INSPECTION_DATE, SRC.NEXT_INSPECTION_DATE, SRC.SUPPLIER_CODE,
            SRC.WAREHOUSE_LOCATION, SRC.QUANTITY_ON_HAND, SRC.REORDER_POINT,
            SRC.AIRCRAFT_MODEL, SRC.COMPLIANCE_REGION, SRC.RISK_SCORE, 'Active',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );

    -- Soft delete: mark target records not in current source extract
    UPDATE gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET
        SET STATUS = 'Decommissioned',
            UPDATED_ON = CURRENT_TIMESTAMP()
    WHERE PART_NUMBER NOT IN (
        SELECT PART_NUMBER FROM gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC
    )
    AND STATUS <> 'Decommissioned';

    -- Count soft deletes
    SELECT CHANGES(INFORMATION => 'ROWS_UPDATED')
    INTO :V_SOFT_DELETED_COUNT
    FROM gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET;

    -- Post-merge target count
    SELECT COUNT(*) INTO :V_TARGET_COUNT
    FROM gen_ai_poc_snowflakecoe.sdlc_wizard.AEROSPACE_PARTS_TARGET;

    -- Derive inserted and updated counts
    -- New rows = total target rows - pre-merge target rows
    V_INSERTED_COUNT := V_TARGET_COUNT - V_PRE_MERGE_TARGET_COUNT;
    -- If inserted count goes negative (shouldn't happen), floor at 0
    IF (V_INSERTED_COUNT < 0) THEN
        V_INSERTED_COUNT := 0;
    END IF;
    -- Updated = source count - inserted (those that matched)
    V_UPDATED_COUNT := V_SOURCE_COUNT - V_INSERTED_COUNT;
    IF (V_UPDATED_COUNT < 0) THEN
        V_UPDATED_COUNT := 0;
    END IF;

    -- Calculate duration
    V_DURATION_SECONDS := DATEDIFF(SECOND, V_START_TS, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);

    -- Insert reconciliation log
    INSERT INTO gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, SOURCE_COUNT, TARGET_COUNT,
        INSERTED_COUNT, UPDATED_COUNT, SOFT_DELETED_COUNT, STATUS,
        DURATION_SECONDS, ERROR_MESSAGE
    ) VALUES (
        :V_RUN_ID, 'SP_LOAD_AEROSPACE_PARTS_SCD1', :V_START_TS, :V_SOURCE_COUNT,
        :V_TARGET_COUNT, :V_INSERTED_COUNT, :V_UPDATED_COUNT,
        :V_SOFT_DELETED_COUNT, 'SUCCESS', :V_DURATION_SECONDS, NULL
    );

    -- Update control table with last successful run
    MERGE INTO gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL AS CTL
    USING (SELECT 'SP_LOAD_AEROSPACE_PARTS_SCD1' AS PIPELINE_NAME) AS SRC
        ON CTL.PIPELINE_NAME = SRC.PIPELINE_NAME
    WHEN MATCHED THEN UPDATE
        SET CTL.LAST_RUN_TIMESTAMP = CURRENT_TIMESTAMP(),
            CTL.LAST_STATUS = 'SUCCESS',
            CTL.UPDATED_BY = CURRENT_USER(),
            CTL.UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, LAST_RUN_TIMESTAMP, LAST_STATUS, UPDATED_BY, UPDATED_AT)
        VALUES ('SP_LOAD_AEROSPACE_PARTS_SCD1', CURRENT_TIMESTAMP(), 'SUCCESS', CURRENT_USER(), CURRENT_TIMESTAMP());

    -- Drop temp table
    DROP TABLE IF EXISTS gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC;

    V_SUMMARY := 'SP_LOAD_AEROSPACE_PARTS_SCD1 completed | RunID=' || V_RUN_ID
        || ' | Source=' || V_SOURCE_COUNT::VARCHAR
        || ' | Inserted=' || V_INSERTED_COUNT::VARCHAR
        || ' | Updated=' || V_UPDATED_COUNT::VARCHAR
        || ' | SoftDeleted=' || V_SOFT_DELETED_COUNT::VARCHAR
        || ' | TargetTotal=' || V_TARGET_COUNT::VARCHAR
        || ' | Duration=' || V_DURATION_SECONDS::VARCHAR || 's'
        || ' | Status=SUCCESS';

    RETURN V_SUMMARY;

EXCEPTION
    WHEN OTHER THEN
        V_ERROR_MSG := SQLERRM;
        V_DURATION_SECONDS := DATEDIFF(SECOND, V_START_TS, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);

        INSERT INTO gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_RECONCILIATION_LOG (
            RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, SOURCE_COUNT, TARGET_COUNT,
            INSERTED_COUNT, UPDATED_COUNT, SOFT_DELETED_COUNT, STATUS,
            DURATION_SECONDS, ERROR_MESSAGE
        ) VALUES (
            :V_RUN_ID, 'SP_LOAD_AEROSPACE_PARTS_SCD1', :V_START_TS, :V_SOURCE_COUNT,
            :V_TARGET_COUNT, :V_INSERTED_COUNT, :V_UPDATED_COUNT,
            :V_SOFT_DELETED_COUNT, 'FAILURE', :V_DURATION_SECONDS, :V_ERROR_MSG
        );

        MERGE INTO gen_ai_poc_snowflakecoe.sdlc_wizard.ETL_CONTROL AS CTL2
        USING (SELECT 'SP_LOAD_AEROSPACE_PARTS_SCD1' AS PIPELINE_NAME) AS SRC2
            ON CTL2.PIPELINE_NAME = SRC2.PIPELINE_NAME
        WHEN MATCHED THEN UPDATE
            SET CTL2.LAST_STATUS = 'FAILURE',
                CTL2.UPDATED_BY = CURRENT_USER(),
                CTL2.UPDATED_AT = CURRENT_TIMESTAMP();

        DROP TABLE IF EXISTS gen_ai_poc_snowflakecoe.sdlc_wizard.TEMP_AERO_SRC;

        RETURN OBJECT_CONSTRUCT(
            'error', V_ERROR_MSG,
            'run_id', V_RUN_ID,
            'status', 'FAILURE',
            'duration_seconds', V_DURATION_SECONDS
        )::VARCHAR;
$$;