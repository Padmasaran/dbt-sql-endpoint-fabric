-- =============================================================================
-- BRONZE → SILVER  |  cost_centre
-- =============================================================================
-- Layer     : Silver (Lakehouse — Silver_Lakehouse.silver)
-- Source    : Bronze_Lakehouse.dbo.cost_centre  (raw Delta table)
-- Purpose   : Clean view over the cost centre dimension.
--             Filters out decommissioned cost centres (is_active = 0)
--             so they do not propagate to Silver or Gold layers.
--
-- Medallion role
--   Bronze  — raw Delta tables, no transformation, loaded by PySpark notebook
--   Silver  — this view; applies light cleansing / business rules
--   Gold    — aggregated / consumption-ready views in Gold_Warehouse
--
-- dbt writes this as a VIEW on the Fabric SQL Endpoint of Silver_Lakehouse.
-- =============================================================================

{{
    config(
        materialized = 'view'
    )
}}

SELECT
    cost_centre_id,
    cost_centre_code,
    cost_centre_name,
    department,
    is_active,
    created_date,
    modified_date
FROM {{ source('bronze', 'cost_centre') }}
WHERE is_active = 1
