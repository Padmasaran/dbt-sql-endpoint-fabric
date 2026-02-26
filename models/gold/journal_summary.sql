-- =============================================================================
-- SILVER → GOLD  |  journal_summary
-- =============================================================================
-- Layer     : Gold (Warehouse — Gold_Warehouse.gold)
-- Source    : Silver_Lakehouse.silver.journal_entry_enriched  (via ref)
-- Purpose   : Aggregated journal summary grouped by department, posting type,
--             currency and date.  Consumption-ready for reporting tools
--             (Power BI, Excel, SQL queries).
--
-- Medallion role
--   Bronze  — raw Delta tables, no transformation, loaded by PySpark notebook
--   Silver  — cleansed and enriched views in Silver_Lakehouse (SQL Endpoint)
--   Gold    — this view; aggregated, business-facing layer in Gold_Warehouse
--               sits on a Fabric Data Warehouse SQL Endpoint, not a Lakehouse
--
-- Cross-workspace lineage
--   Bronze_Lakehouse  →  Silver_Lakehouse  →  Gold_Warehouse
--   dbt tracks this full chain automatically via source() and ref().
--   Column-level lineage is visible in the dbt docs DAG with no extra tooling.
--
-- Why this beats SQL Database Projects for this pattern
--   - Dependency order is inferred from ref() — no manual script ordering
--   - Environment targets (dev / prod) flip via env_var() in dbt_project.yml
--   - Full lineage from Bronze raw table to this Gold aggregate out of the box
-- =============================================================================

{{
    config(
        materialized = 'view'
    )
}}

SELECT
    department,
    posting_type_code,
    posting_type_description,
    currency_code,
    journal_date,

    COUNT(journal_entry_id) AS entry_count,
    SUM(amount)             AS total_amount,
    AVG(amount)             AS avg_amount,
    MIN(amount)             AS min_amount,
    MAX(amount)             AS max_amount

FROM {{ ref('journal_entry_enriched') }}

GROUP BY
    department,
    posting_type_code,
    posting_type_description,
    currency_code,
    journal_date
