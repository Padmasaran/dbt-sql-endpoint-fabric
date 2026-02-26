-- =============================================================================
-- generate_schema_name
-- =============================================================================
-- Overrides dbt's default schema naming behaviour.
--
-- By default dbt concatenates the target schema onto any custom schema
-- defined in dbt_project.yml, producing names like "dbo_silver".
-- This macro returns the custom schema name as-is so models land in exactly
-- the schema you specify (e.g. "silver", "gold").
--
-- This is required when writing to a Fabric SQL Endpoint because the schema
-- name must match what is configured in the Lakehouse / Warehouse exactly.
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
