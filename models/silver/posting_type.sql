-- =============================================================================
-- BRONZE → SILVER  |  posting_type
-- =============================================================================
-- Layer     : Silver (Lakehouse — Silver_Lakehouse.silver)
-- Source    : Bronze_Lakehouse.dbo.posting_type  (raw Delta table)
-- Purpose   : Clean view over the posting type reference data.
--             Filters out inactive codes so downstream models only see
--             valid posting types.
--
-- Medallion role
--   Bronze  — raw Delta tables, no transformation, loaded by PySpark notebook
--   Silver  — this view; applies light cleansing / business rules
--   Gold    — aggregated / consumption-ready views in Gold_Warehouse
--
-- dbt writes this as a VIEW on the Fabric SQL Endpoint of Silver_Lakehouse.
-- The ref() macro resolves cross-layer dependencies automatically — no manual
-- dependency wiring needed.
-- =============================================================================

{{
    config(
        materialized = 'view'
    )
}}

SELECT
    posting_type_id,
    posting_type_code,
    posting_type_description,
    is_active,
    created_date,
    modified_date
FROM {{ source('bronze', 'posting_type') }}
WHERE is_active = 1
