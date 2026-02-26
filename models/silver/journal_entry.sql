-- =============================================================================
-- BRONZE → SILVER  |  journal_entry
-- =============================================================================
-- Layer     : Silver (Lakehouse — Silver_Lakehouse.silver)
-- Source    : Bronze_Lakehouse.dbo.journal_entry  (raw Delta table)
-- Purpose   : Clean view over raw journal entry transactions.
--             Excludes rows with a NULL amount — these are incomplete entries
--             that should not flow into reporting or aggregation.
--
-- Medallion role
--   Bronze  — raw Delta tables, no transformation, loaded by PySpark notebook
--   Silver  — this view; applies light cleansing / business rules
--   Gold    — aggregated / consumption-ready views in Gold_Warehouse
--
-- dbt writes this as a VIEW on the Fabric SQL Endpoint of Silver_Lakehouse.
-- journal_entry_enriched (Silver Layer 2) depends on this model via ref().
-- dbt infers that dependency automatically from the ref() call — no manual
-- ordering required.
-- =============================================================================

{{
    config(
        materialized = 'view'
    )
}}

SELECT
    journal_entry_id,
    journal_date,
    posting_type_id,
    cost_centre_id,
    amount,
    currency_code,
    description,
    created_date,
    modified_date
FROM {{ source('bronze', 'journal_entry') }}
WHERE amount IS NOT NULL
