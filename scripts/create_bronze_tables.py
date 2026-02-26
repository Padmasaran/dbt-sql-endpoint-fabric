# =============================================================================
# create_bronze_tables.py
# =============================================================================
# PREREQUISITE SETUP SCRIPT — run this before executing any dbt commands.
#
# Where to run
#   Open a PySpark notebook in Microsoft Fabric attached to Bronze_Lakehouse.
#   Copy each cell block below into a separate notebook cell and run top-to-bottom.
#
# What it creates (three Delta tables in the dbo schema of Bronze_Lakehouse)
#   dbo.posting_type   — reference / lookup table
#   dbo.cost_centre    — cost centre dimension
#   dbo.journal_entry  — transactional fact table  (one row has NULL amount
#                        intentionally — Silver layer filters it out)
#
# Why this is needed
#   dbt reads from the SQL Endpoint of Bronze_Lakehouse.  The SQL Endpoint
#   exposes Delta tables as SQL-queryable objects.  These three tables must
#   exist before `dbt run` is executed, otherwise the source() references in
#   models/silver/sources.yml will resolve to missing objects.
#
# Medallion context
#   Bronze  — this script; raw data landed as Delta tables, no transformation
#   Silver  — dbt views that clean and enrich the Bronze tables
#   Gold    — dbt views that aggregate Silver into consumption-ready outputs
# =============================================================================


# ── Cell 1: Config ────────────────────────────────────────────────────────────
# Schema to create tables in.  Must match DBT_BRONZE_SCHEMA (default: dbo).
SCHEMA = "dbo"


# ── Cell 2: Create posting_type ───────────────────────────────────────────────
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, BooleanType, TimestampType,
)
from datetime import datetime

posting_type_schema = StructType([
    StructField("posting_type_id",          IntegerType(),   False),
    StructField("posting_type_code",        StringType(),    False),
    StructField("posting_type_description", StringType(),    True),
    StructField("is_active",                BooleanType(),   False),
    StructField("created_date",             TimestampType(), True),
    StructField("modified_date",            TimestampType(), True),
])

posting_type_data = [
    Row(1, "ACCRUAL",  "Accrual posting",         True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(2, "REVERSAL", "Reversal of prior entry",  True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(3, "MANUAL",   "Manual journal entry",     True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(4, "SYSTEM",   "System-generated entry",   True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(5, "LEGACY",   "Legacy system entry",      False, datetime(2024, 1, 1), datetime(2024, 6, 1)),
    # LEGACY (is_active=False) is filtered out by the Silver posting_type view
]

df_posting_type = spark.createDataFrame(posting_type_data, schema=posting_type_schema)
df_posting_type.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA}.posting_type")

print(f"✓ {SCHEMA}.posting_type created — {df_posting_type.count()} rows")
df_posting_type.show()


# ── Cell 3: Create cost_centre ────────────────────────────────────────────────
cost_centre_schema = StructType([
    StructField("cost_centre_id",   IntegerType(),   False),
    StructField("cost_centre_code", StringType(),    False),
    StructField("cost_centre_name", StringType(),    True),
    StructField("department",       StringType(),    True),
    StructField("is_active",        BooleanType(),   False),
    StructField("created_date",     TimestampType(), True),
    StructField("modified_date",    TimestampType(), True),
])

cost_centre_data = [
    Row(1, "CC001", "Finance Operations",  "Finance",    True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(2, "CC002", "Sales EMEA",          "Sales",      True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(3, "CC003", "Sales APAC",          "Sales",      True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(4, "CC004", "IT Infrastructure",   "Technology", True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(5, "CC005", "HR & People",         "HR",         True,  datetime(2024, 1, 1), datetime(2024, 1, 1)),
    Row(6, "CC006", "Decommissioned Dept", "Legacy",     False, datetime(2023, 1, 1), datetime(2024, 3, 1)),
    # Decommissioned centre (is_active=False) is filtered out by the Silver cost_centre view
]

df_cost_centre = spark.createDataFrame(cost_centre_data, schema=cost_centre_schema)
df_cost_centre.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA}.cost_centre")

print(f"✓ {SCHEMA}.cost_centre created — {df_cost_centre.count()} rows")
df_cost_centre.show()


# ── Cell 4: Create journal_entry ──────────────────────────────────────────────
from pyspark.sql.types import DoubleType, DateType
from datetime import date

journal_entry_schema = StructType([
    StructField("journal_entry_id", IntegerType(),   False),
    StructField("journal_date",     DateType(),      False),
    StructField("posting_type_id",  IntegerType(),   True),
    StructField("cost_centre_id",   IntegerType(),   True),
    StructField("amount",           DoubleType(),    True),
    StructField("currency_code",    StringType(),    True),
    StructField("description",      StringType(),    True),
    StructField("created_date",     TimestampType(), True),
    StructField("modified_date",    TimestampType(), True),
])

journal_entry_data = [
    Row(1,  date(2024, 1, 15), 1, 1,  1500.00, "GBP", "Q1 accrual - Finance Ops",    datetime(2024, 1, 15), datetime(2024, 1, 15)),
    Row(2,  date(2024, 1, 20), 3, 2,  2300.50, "GBP", "Manual adj - Sales EMEA",      datetime(2024, 1, 20), datetime(2024, 1, 20)),
    Row(3,  date(2024, 2, 1),  2, 1, -1500.00, "GBP", "Reversal of entry 1",          datetime(2024, 2, 1),  datetime(2024, 2, 1)),
    Row(4,  date(2024, 2, 10), 4, 4,  8750.00, "GBP", "System entry - IT infra",      datetime(2024, 2, 10), datetime(2024, 2, 10)),
    Row(5,  date(2024, 2, 14), 1, 3,  3200.00, "USD", "Q1 accrual - Sales APAC",      datetime(2024, 2, 14), datetime(2024, 2, 14)),
    Row(6,  date(2024, 3, 1),  3, 5,   450.75, "GBP", "Manual adj - HR & People",     datetime(2024, 3, 1),  datetime(2024, 3, 1)),
    Row(7,  date(2024, 3, 15), 1, 2,  5100.00, "GBP", "Q1 accrual - Sales EMEA",      datetime(2024, 3, 15), datetime(2024, 3, 15)),
    Row(8,  date(2024, 3, 31), 4, 1,  1200.00, "GBP", "System close - Finance Ops",   datetime(2024, 3, 31), datetime(2024, 3, 31)),
    Row(9,  date(2024, 4, 5),  3, 3,  2800.00, "USD", "Manual adj - Sales APAC",      datetime(2024, 4, 5),  datetime(2024, 4, 5)),
    Row(10, date(2024, 4, 10), 1, 4,  9500.00, "GBP", "Q2 accrual - IT infra",        datetime(2024, 4, 10), datetime(2024, 4, 10)),
    Row(11, date(2024, 4, 20), 3, 2,  None,    "GBP", "Incomplete entry - excluded",  datetime(2024, 4, 20), datetime(2024, 4, 20)),
    # Row 11 has NULL amount — intentionally excluded by Silver journal_entry (WHERE amount IS NOT NULL)
]

df_journal_entry = spark.createDataFrame(journal_entry_data, schema=journal_entry_schema)
df_journal_entry.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA}.journal_entry")

print(f"✓ {SCHEMA}.journal_entry created — {df_journal_entry.count()} rows (1 excluded by Silver filter)")
df_journal_entry.show()


# ── Cell 5: Verify ────────────────────────────────────────────────────────────
print("=== Tables in schema:", SCHEMA, "===")
spark.sql(f"SHOW TABLES IN {SCHEMA}").show()

print("\n=== Row counts ===")
for table in ["posting_type", "cost_centre", "journal_entry"]:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.{table}").collect()[0]["cnt"]
    print(f"  {SCHEMA}.{table}: {count} rows")
