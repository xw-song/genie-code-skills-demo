---
name: table-governance
description: Enforce table and column documentation standards across all tables. Use for every table you create or modify. Covers COMMENT clauses, TBLPROPERTIES, column descriptions, PII labeling at the table level, and Unity Catalog tags. This is the baseline governance skill -- apply it before any other skill.
---

# Table and Column Governance Standards

Apply these documentation and governance rules to **every** table, regardless of layer or domain. This skill takes priority and should be applied first, before domain-specific skills like `sdp-basics` or `pii-management`.

## Table-Level Documentation

### COMMENT Clause (Required)

Every table MUST have a `COMMENT` clause. The comment must describe:
1. What the table contains
2. The data source or upstream table
3. Whether the table contains PII (if applicable)

| Layer | Pattern |
|-------|---------|
| Bronze | `COMMENT "Raw <entity> data ingested from <source>"` |
| Silver | `COMMENT "Cleaned and validated <entity> with derived metrics from bronze_<entity>"` |
| Gold | `COMMENT "Business aggregation: <metric> by <dimensions>"` |
| PII | Append ` - CONTAINS PII: <column_list>` to any table with personal data |

### TBLPROPERTIES (Required)

Every table MUST have `TBLPROPERTIES` with at minimum:

```sql
TBLPROPERTIES (
  "quality" = "<bronze|silver|gold>",
  "data_owner" = "<team-or-domain>",
  "domain" = "<business-domain>"
)
```

> Note: use `data_owner`, not `owner`. `owner` is a reserved Databricks table property and is rejected inside `TBLPROPERTIES`.

Additional required properties by context:

| Condition | Additional Properties |
|-----------|-----------------------|
| Contains PII | `"contains_pii" = "true"`, `"pii_columns" = "<column_list>"` |
| Silver/Gold layer | `"delta.enableChangeDataFeed" = "true"` |
| Streaming table | `"delta.enableRowTracking" = "true"` |
| Subject to retention | `"retention_days" = "<number>"` |

## Column-Level Documentation

### Column Descriptions (inline in the CREATE)

In SDP pipelines, add column descriptions **inline in the table's column list** inside the `CREATE OR REFRESH ...` statement. Do NOT use `ALTER TABLE ... ALTER COLUMN ... COMMENT` -- imperative `ALTER TABLE` is not allowed in pipeline source files. List the columns in the same order as the `SELECT` and attach a `COMMENT` to each documented column:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id        COMMENT 'Unique customer identifier from source system',
  income_tier        COMMENT 'Derived income bracket: High Income / Upper Middle / Middle / Lower Middle',
  email_hash         COMMENT 'SHA-256 hash of lowercase trimmed email for matching without PII exposure',
  data_quality_flag  COMMENT 'Row-level DQ status: CLEAN, MISSING_<field>, or NEGATIVE_<field>',
  audit_timestamp    COMMENT 'Pipeline execution timestamp',
  source_system      COMMENT 'Upstream source system identifier'
)
COMMENT "..."
TBLPROPERTIES (...)
AS SELECT ...;
```

### Which Columns Need Descriptions

At minimum, add descriptions for:
- Primary keys and foreign keys
- Derived or calculated columns (explain the logic)
- PII columns (note the risk level and any masking applied)
- Data quality flag columns
- Audit columns
- Any column whose meaning is not obvious from its name

### Column Description Patterns

| Column Type | Description Pattern |
|-------------|---------------------|
| Primary key | `'Unique <entity> identifier from <source>'` |
| Foreign key | `'References <parent_table>.<parent_column>'` |
| Derived numeric | `'Calculated as <formula>. Units: <unit>'` |
| Derived category | `'Derived <category> bracket: <value1> / <value2> / ...'` |
| Masked PII | `'<Masking method> of <original_column> for PII protection'` |
| Audit timestamp | `'Pipeline execution timestamp'` |
| Source system | `'Upstream source system identifier'` |
| DQ flag | `'Row-level data quality status: <possible values>'` |

## PII Labeling at Table Level

Tables containing personal data require additional governance:

1. **TBLPROPERTIES** must include `"contains_pii" = "true"` and `"pii_columns"` listing all PII column names
2. **COMMENT** must include `CONTAINS PII: <column_list>`
3. **Column descriptions** for PII columns must note the PII type and risk level
4. **Governance metadata is captured inline via TBLPROPERTIES** (`contains_pii`, `pii_columns`, plus `quality` / `domain` / `data_owner`). The equivalent Unity Catalog **tags** are applied by governance automation *outside* the pipeline -- `ALTER TABLE ... SET TAGS` is not valid in pipeline source, so do NOT emit it in pipeline files. Within the pipeline, rely on TBLPROPERTIES for this metadata.

## Unity Catalog Tags (applied outside the pipeline)

Tags provide discoverability and governance automation. They are applied **after** the pipeline creates the table -- by governance automation, a SQL notebook, or a CI step (`ALTER TABLE ... SET TAGS`) -- and are never emitted in pipeline source. Within the pipeline, capture the same metadata in TBLPROPERTIES (above). Reference tag set:

| Tag | Values | Purpose |
|-----|--------|---------|
| `quality` | `bronze`, `silver`, `gold` | Layer classification |
| `domain` | `finance`, `hr`, `marketing`, etc. | Business domain |
| `pii` | `true`, `false` | PII flag for governance scanning |
| `data_classification` | `public`, `internal`, `confidential`, `restricted` | Access control tier |
| `owner` | `<team-name>` | Ownership for accountability |
| `sla` | `daily`, `hourly`, `real-time` | Freshness expectation |

## Governance Checklist

Before completing any table definition, verify ALL of the following:

- [ ] `COMMENT` clause is present and descriptive
- [ ] `TBLPROPERTIES` includes at least `quality` and `data_owner`
- [ ] If PII is present: `contains_pii` and `pii_columns` are in TBLPROPERTIES
- [ ] If PII is present: COMMENT mentions `CONTAINS PII`
- [ ] Column descriptions added inline (in the CREATE column list) for primary keys, derived columns, and PII columns
- [ ] Unity Catalog tags applied post-deploy (`quality`, `domain`, `pii`, `data_classification`)
- [ ] `audit_timestamp` and `source_system` are the last two columns

## Example: Full Governance Application

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  customer_id        COMMENT 'Unique customer identifier from CRM',
  region,
  customer_segment,
  email_hash         COMMENT 'SHA-256 hash of lowercase trimmed email for matching without PII exposure',
  phone_masked       COMMENT 'Last 4 digits of phone number, masked for PII protection',
  age                COMMENT 'Derived age in years from date_of_birth',
  income_tier        COMMENT 'Derived income bracket: High Income / Upper Middle / Middle / Lower Middle',
  data_quality_flag  COMMENT 'Row-level DQ status: CLEAN or MISSING_CUSTOMER_ID',
  audit_timestamp    COMMENT 'Pipeline execution timestamp',
  source_system      COMMENT 'Upstream source system identifier',
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Cleaned customer data with derived tiers from bronze_customers - CONTAINS PII: email_hash, phone_masked, age"
TBLPROPERTIES (
  "quality" = "silver",
  "data_owner" = "data-engineering",
  "domain" = "customer",
  "contains_pii" = "true",
  "pii_columns" = "email_hash,phone_masked,age",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  customer_id,
  region,
  customer_segment,
  SHA2(LOWER(TRIM(email)), 256) AS email_hash,
  CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)) AS phone_masked,
  FLOOR(DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365) AS age,
  CASE
    WHEN annual_income >= 250000 THEN 'High Income'
    WHEN annual_income >= 100000 THEN 'Upper Middle'
    WHEN annual_income >= 50000 THEN 'Middle'
    ELSE 'Lower Middle'
  END AS income_tier,
  CASE
    WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER_ID'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS audit_timestamp,
  'crm_system' AS source_system
FROM LIVE.bronze_customers;

-- UC tags are applied POST-DEPLOY (not valid in pipeline source):
-- ALTER TABLE silver_customers
--   SET TAGS ('pii' = 'true', 'data_classification' = 'confidential', 'domain' = 'customer');
```
