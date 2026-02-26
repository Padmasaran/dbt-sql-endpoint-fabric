-- =============================================================================
-- SILVER LAYER 2  |  journal_entry_enriched
-- =============================================================================
-- Layer     : Silver (Lakehouse — Silver_Lakehouse.silver)
-- Sources   : Three Silver Layer 1 views resolved by dbt ref()
--               - journal_entry      (fact)
--               - posting_type       (reference / lookup)
--               - cost_centre        (dimension)
-- Purpose   : Enriched journal entries — joins fact rows with their posting
--             type label and cost centre details in a single denormalised view.
--             This is the primary input for the Gold aggregation layer.
--
-- Medallion role
--   Bronze  — raw Delta tables, no transformation, loaded by PySpark notebook
--   Silver  — Light cleansing views (Layer 1) + this enriched join (Layer 2)
--   Gold    — Aggregated, consumption-ready views in Gold_Warehouse
--               journal_summary reads from this model via ref()
--
-- Key dbt benefit demonstrated here
--   All three ref() calls create an explicit dependency graph.
--   dbt guarantees that posting_type, cost_centre, and journal_entry are
--   materialised before this model runs — zero manual dependency wiring.
-- =============================================================================

{{
    config(
        materialized = 'view'
    )
}}

SELECT
    je.journal_entry_id,
    je.journal_date,
    je.amount,
    je.currency_code,
    je.description,

    -- Posting type detail (resolved from Silver Layer 1 via ref)
    pt.posting_type_code,
    pt.posting_type_description,

    -- Cost centre detail (resolved from Silver Layer 1 via ref)
    cc.cost_centre_code,
    cc.cost_centre_name,
    cc.department,

    je.created_date,
    je.modified_date

FROM {{ ref('journal_entry') }} je
LEFT JOIN {{ ref('posting_type') }} pt ON je.posting_type_id = pt.posting_type_id
LEFT JOIN {{ ref('cost_centre') }}  cc ON je.cost_centre_id  = cc.cost_centre_id
