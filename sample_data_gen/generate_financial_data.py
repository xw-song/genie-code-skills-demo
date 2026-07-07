# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Financial Data
# MAGIC
# MAGIC This notebook generates synthetic financial services data for the Genie Code skills demo
# MAGIC using [dbldatagen](https://github.com/databrickslabs/dbldatagen) (Databricks Labs Data Generator).
# MAGIC All data is randomly generated -- no real customer data is used.
# MAGIC
# MAGIC **Generated Datasets:**
# MAGIC - `branches` - Branch/location reference data (200 rows)
# MAGIC - `products` - Financial products catalog (50 rows)
# MAGIC - `date_dimensions` - Date dimension table (3 years)
# MAGIC - `customers` - Customer master data with PII (50,000 rows)
# MAGIC - `accounts` - Account information (75,000 rows)
# MAGIC - `transactions` - Financial transactions (500,000 rows)
# MAGIC
# MAGIC **Output:** CSV files written to `/Volumes/{catalog}/{schema}/{volume}/`

# COMMAND ----------

# MAGIC %pip install dbldatagen --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")
dbutils.widgets.text("volume", "raw_data", "Volume Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_NAME = dbutils.widgets.get("volume")

if not CATALOG or not SCHEMA:
    raise ValueError(
        "Please provide catalog and schema. "
        "Run: dbutils.widgets.text('catalog', 'my_catalog')"
    )

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

print(f"Output path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Schema and Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

import dbldatagen as dg
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, DateType, DoubleType, TimestampType
)
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Branches

# COMMAND ----------

cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Jacksonville", "San Jose", "Columbus", "Charlotte", "Indianapolis",
    "San Francisco", "Seattle", "Denver", "Nashville", "Portland",
    "Boston", "Atlanta", "Miami", "Minneapolis",
]

branch_suffixes = ["Main Street", "Central", "Business Park", "Mall", "Station"]

branch_gen = (
    dg.DataGenerator(spark, name="branches", rows=200, partitions=4)
    .withColumn("branch_id", StringType(),
                expr="concat('BR', lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0'))")
    .withColumn("branch_name", StringType(), values=branch_suffixes)
    .withColumn("branch_type", StringType(),
                expr="CASE WHEN pmod(hash(id,'branch_type'),1000) < 400 THEN 'Full Service' WHEN pmod(hash(id,'branch_type'),1000) < 650 THEN 'Express' WHEN pmod(hash(id,'branch_type'),1000) < 800 THEN 'Digital Hub' WHEN pmod(hash(id,'branch_type'),1000) < 900 THEN 'Business Center' ELSE 'Premium' END")
    .withColumn("city", StringType(), values=cities)
    .withColumn("region", StringType(),
                values=["North", "South", "East", "West", "Central", "Metro", "Pacific", "Mountain", "Midwest"])
    .withColumn("country", StringType(), values=["United States"])
    .withColumn("postal_code", StringType(),
                expr="lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0')")
    .withColumn("phone", StringType(),
                expr="concat('+1 ', lpad(cast(cast(floor(rand()*900+100) as int) as string),3,'0'), '-', lpad(cast(cast(floor(rand()*900+100) as int) as string),3,'0'), '-', lpad(cast(cast(floor(rand()*10000) as int) as string),4,'0'))")
    .withColumn("manager_name", StringType(),
                expr="concat(element_at(array('James','Mary','John','Patricia','Robert','Linda','Michael','Barbara'), cast(floor(rand()*8)+1 as int)), ' ', element_at(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis'), cast(floor(rand()*8)+1 as int)))")
    .withColumn("num_employees", IntegerType(), minValue=5, maxValue=50)
    .withColumn("opened_date", DateType(),
                expr="date_add(to_date('2000-01-01'), cast(rand()*8000 as int))")
    .withColumn("is_24h_atm", StringType(), values=["Y", "N"])
    .withColumn("has_safe_deposit", StringType(), values=["Y", "N"])
    .withColumn("has_business_services", StringType(), values=["Y", "N"])
    .withColumn("status", StringType(),
                expr="CASE WHEN pmod(hash(id,'status'),1000) < 950 THEN 'Active' WHEN pmod(hash(id,'status'),1000) < 980 THEN 'Inactive' ELSE 'Renovation' END")
)

branches_df = branch_gen.build()
branches_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/branches")
print(f"Generated {branches_df.count()} branches")
branches_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products

# COMMAND ----------

product_names = [
    "Basic Checking", "Premium Checking", "Student Checking", "Business Checking", "Senior Checking",
    "Regular Savings", "High-Yield Savings", "Money Market", "CD 6-Month", "CD 12-Month", "CD 24-Month",
    "Cashback Card", "Travel Rewards", "Business Card", "Platinum Card", "Student Card", "Secured Card",
    "Personal Loan", "Auto Loan", "Home Equity", "Business Loan", "Student Loan",
    "Fixed 15-Year", "Fixed 30-Year", "Adjustable Rate", "FHA Loan", "VA Loan",
    "Brokerage Account", "IRA Traditional", "IRA Roth", "Managed Portfolio", "529 Plan",
]

categories_map = (
    ["Checking"] * 5 + ["Savings"] * 6 + ["Credit Card"] * 6 +
    ["Loan"] * 5 + ["Mortgage"] * 5 + ["Investment"] * 5
)

product_gen = (
    dg.DataGenerator(spark, name="products", rows=len(product_names), partitions=1)
    .withColumn("product_id", StringType(),
                expr="concat('PROD', lpad(cast(cast(floor(rand()*10000) as int) as string), 4, '0'))")
    .withColumn("product_name", StringType(), values=product_names)
    .withColumn("product_category", StringType(), values=categories_map)
    .withColumn("interest_rate_pct", FloatType(), minValue=0.01, maxValue=5.5, percentNulls=0.4)
    .withColumn("apr_pct", FloatType(), minValue=3.5, maxValue=24.9, percentNulls=0.5)
    .withColumn("min_balance", IntegerType(),
                values=[0, 100, 500, 1000, 5000, 10000], percentNulls=0.5)
    .withColumn("monthly_fee", IntegerType(),
                values=[0, 0, 0, 5, 10, 15, 25], percentNulls=0.5)
    .withColumn("annual_fee", IntegerType(),
                values=[0, 0, 0, 50, 95, 150, 450], percentNulls=0.7)
    .withColumn("reward_rate_pct", FloatType(), minValue=1.0, maxValue=5.0, percentNulls=0.7)
    .withColumn("term_months", IntegerType(),
                values=[6, 12, 24, 36, 60, 120, 180, 360], percentNulls=0.5)
    .withColumn("fdic_insured", StringType(), values=["Y", "N"])
    .withColumn("min_credit_score", IntegerType(), minValue=580, maxValue=750, percentNulls=0.4)
    .withColumn("launch_date", DateType(),
                expr="date_add(to_date('2010-01-01'), cast(rand()*5000 as int))")
    .withColumn("status", StringType(),
                expr="CASE WHEN pmod(hash(id,'status'),1000) < 850 THEN 'Active' WHEN pmod(hash(id,'status'),1000) < 950 THEN 'Discontinued' ELSE 'Limited' END")
)

products_df = product_gen.build()

products_df = (
    products_df
    .withColumn("product_type",
                F.when(F.col("product_category").isin("Savings", "Checking", "Investment"), "Asset")
                .otherwise("Liability"))
)

products_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/products")
print(f"Generated {products_df.count()} products")
products_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Date Dimensions

# COMMAND ----------

date_gen = (
    dg.DataGenerator(spark, name="date_dimensions", rows=1096, partitions=2)
    .withColumn("full_date", DateType(),
                expr="date_add(to_date('2023-01-01'), cast(id as int))")
)

dates_df = (
    date_gen.build()
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("calendar_year", F.year("full_date"))
    .withColumn("calendar_quarter", F.quarter("full_date"))
    .withColumn("calendar_quarter_name", F.concat(F.lit("Q"), F.quarter("full_date"), F.lit(" "), F.year("full_date")))
    .withColumn("month_number", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("month_short", F.date_format("full_date", "MMM"))
    .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week_number", F.dayofweek("full_date"))
    .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
    .withColumn("day_of_week_short", F.date_format("full_date", "EEE"))
    .withColumn("fiscal_year",
                F.when(F.month("full_date") >= 4, F.year("full_date"))
                .otherwise(F.year("full_date") - 1))
    .withColumn("fiscal_quarter",
                ((F.month("full_date") - 4 + 12) % 12 / 3).cast(IntegerType()) + 1)
    .withColumn("is_weekend",
                F.when(F.dayofweek("full_date").isin(1, 7), "Y").otherwise("N"))
    .withColumn("is_month_end",
                F.when(F.last_day("full_date") == F.col("full_date"), "Y").otherwise("N"))
    .withColumn("is_year_end",
                F.when((F.month("full_date") == 12) & (F.dayofmonth("full_date") == 31), "Y")
                .otherwise("N"))
    .drop("id")
)

dates_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/date_dimensions")
print(f"Generated {dates_df.count()} date records")
dates_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers

# COMMAND ----------

first_names = [
    "James", "William", "Oliver", "George", "Harry", "Jack", "Leo",
    "Oscar", "Charlie", "Henry", "Thomas", "Noah", "Arthur", "Theo",
    "Joshua", "Jacob", "Ethan", "Daniel", "Matthew", "David",
    "Olivia", "Emma", "Charlotte", "Amelia", "Sophia", "Isabella",
    "Mia", "Evelyn", "Harper", "Luna", "Camila", "Sofia", "Emily",
    "Elizabeth", "Ella", "Avery", "Scarlett", "Grace", "Lily", "Victoria",
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis",
    "Garcia", "Rodriguez", "Wilson", "Martinez", "Anderson", "Taylor",
    "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White",
    "Harris", "Clark", "Lewis", "Robinson", "Walker", "Hall", "Allen",
    "Young", "King", "Wright",
]

customer_cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego",
]

streets = [
    "Main Street", "Oak Lane", "Park Road",
    "Church Lane", "Station Road", "Maple Avenue",
]

customer_gen = (
    dg.DataGenerator(spark, name="customers", rows=50000, partitions=8)
    .withColumn("customer_id", StringType(),
                expr="concat('CUST', lpad(cast(cast(floor(rand()*100000000) as bigint) as string), 8, '0'))")
    .withColumn("first_name", StringType(), values=first_names)
    .withColumn("last_name", StringType(), values=last_names)
    .withColumn("email", StringType(),
                expr="concat(lower(first_name), '.', lower(last_name), cast(cast(floor(rand()*900+100) as int) as string), '@example.com')")
    .withColumn("phone", StringType(),
                expr="concat('+1 ', lpad(cast(cast(floor(rand()*900+100) as int) as string),3,'0'), '-', lpad(cast(cast(floor(rand()*900+100) as int) as string),3,'0'), '-', lpad(cast(cast(floor(rand()*10000) as int) as string),4,'0'))")
    .withColumn("date_of_birth", DateType(),
                expr="date_add(to_date('1940-01-01'), cast(rand()*24000 as int))")
    .withColumn("gender", StringType(), values=["M", "F"])
    .withColumn("street_address", StringType(),
                expr="concat(cast(cast(floor(rand()*9900+100) as int) as string), ' ', element_at(array('Main','Oak','Pine','Maple','Cedar','Elm','Washington','Lake','Hill','Park'), cast(floor(rand()*10)+1 as int)), ' Street')")
    .withColumn("city", StringType(), values=customer_cities)
    .withColumn("postal_code", StringType(),
                expr="lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0')")
    .withColumn("country", StringType(), values=["United States"])
    .withColumn("customer_segment", StringType(),
                expr="CASE WHEN pmod(hash(id,'seg'),1000) < 600 THEN 'Mass Market' WHEN pmod(hash(id,'seg'),1000) < 850 THEN 'Mass Affluent' WHEN pmod(hash(id,'seg'),1000) < 950 THEN 'High Net Worth' WHEN pmod(hash(id,'seg'),1000) < 980 THEN 'Ultra High Net Worth' ELSE 'Small Business' END")
    .withColumn("credit_score", IntegerType(), minValue=550, maxValue=850)
    .withColumn("annual_income", DoubleType(), minValue=20000.0, maxValue=500000.0)
    .withColumn("employment_status", StringType(),
                expr="CASE WHEN pmod(hash(id,'emp'),1000) < 650 THEN 'Employed' WHEN pmod(hash(id,'emp'),1000) < 800 THEN 'Self-Employed' WHEN pmod(hash(id,'emp'),1000) < 920 THEN 'Retired' WHEN pmod(hash(id,'emp'),1000) < 970 THEN 'Student' ELSE 'Unemployed' END")
    .withColumn("primary_branch_id", StringType(),
                expr="concat('BR', lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0'))")
    .withColumn("customer_since", DateType(),
                expr="date_add(to_date('2005-01-01'), cast(rand()*7000 as int))")
    .withColumn("kyc_verified", StringType(), expr="CASE WHEN rand() < 0.95 THEN 'Y' ELSE 'N' END")
    .withColumn("marketing_consent", StringType(), values=["Y", "N"])
    .withColumn("digital_banking_enrolled", StringType(), expr="CASE WHEN rand() < 0.85 THEN 'Y' ELSE 'N' END")
    .withColumn("status", StringType(),
                expr="CASE WHEN pmod(hash(id,'cust_status'),1000) < 850 THEN 'Active' WHEN pmod(hash(id,'cust_status'),1000) < 930 THEN 'Inactive' WHEN pmod(hash(id,'cust_status'),1000) < 980 THEN 'Dormant' ELSE 'Closed' END")
)

customers_df = customer_gen.build()

customers_df = customers_df.withColumn(
    "full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
)

customers_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/customers")
print(f"Generated {customers_df.count()} customers")
customers_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Accounts

# COMMAND ----------

account_gen = (
    dg.DataGenerator(spark, name="accounts", rows=75000, partitions=8)
    .withColumn("account_id", StringType(),
                expr="concat('ACC', lpad(cast(cast(floor(rand()*10000000000) as bigint) as string), 10, '0'))")
    .withColumn("account_number", StringType(),
                expr="lpad(cast(cast(floor(rand()*1000000000000) as bigint) as string), 12, '0')")
    .withColumn("customer_id", StringType(),
                expr="concat('CUST', lpad(cast(cast(floor(rand()*100000000) as bigint) as string), 8, '0'))")
    .withColumn("product_id", StringType(),
                expr="concat('PROD', lpad(cast(cast(floor(rand()*10000) as int) as string), 4, '0'))")
    .withColumn("account_type", StringType(),
                expr="CASE WHEN pmod(hash(id,'acct_type'),1000) < 350 THEN 'Checking' WHEN pmod(hash(id,'acct_type'),1000) < 650 THEN 'Savings' WHEN pmod(hash(id,'acct_type'),1000) < 850 THEN 'Credit Card' ELSE 'Investment' END")
    .withColumn("currency", StringType(), values=["USD"])
    .withColumn("current_balance", DoubleType(), minValue=-500.0, maxValue=500000.0)
    .withColumn("available_balance", DoubleType(), minValue=0.0, maxValue=500000.0)
    .withColumn("credit_limit", IntegerType(),
                values=[1000, 2500, 5000, 10000, 25000, 50000], percentNulls=0.6)
    .withColumn("interest_rate_pct", FloatType(), minValue=0.01, maxValue=5.5)
    .withColumn("opened_date", DateType(),
                expr="date_add(to_date('2010-01-01'), cast(rand()*5400 as int))")
    .withColumn("last_activity_date", DateType(),
                expr="date_add(to_date('2023-01-01'), cast(rand()*900 as int))")
    .withColumn("branch_id", StringType(),
                expr="concat('BR', lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0'))")
    .withColumn("is_primary", StringType(), expr="CASE WHEN rand() < 0.30 THEN 'Y' ELSE 'N' END")
    .withColumn("overdraft_protection", StringType(), values=["Y", "N"], percentNulls=0.5)
    .withColumn("paperless_statements", StringType(), expr="CASE WHEN rand() < 0.75 THEN 'Y' ELSE 'N' END")
    .withColumn("status", StringType(),
                expr="CASE WHEN pmod(hash(id,'acct_status'),1000) < 880 THEN 'Active' WHEN pmod(hash(id,'acct_status'),1000) < 940 THEN 'Dormant' WHEN pmod(hash(id,'acct_status'),1000) < 960 THEN 'Frozen' ELSE 'Closed' END")
)

accounts_df = account_gen.build()
accounts_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/accounts")
print(f"Generated {accounts_df.count()} accounts")
accounts_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Transactions

# COMMAND ----------

merchants = [
    "Walmart", "Target", "Amazon", "Apple", "Netflix", "Spotify",
    "Costco", "Kroger", "Starbucks", "McDonalds", "Uber", "DoorDash",
    "Whole Foods", "Home Depot", "Best Buy", "Shell", "Chevron",
    "Verizon", "AT&T", "Comcast",
]

txn_gen = (
    dg.DataGenerator(spark, name="transactions", rows=500000, partitions=16)
    .withColumn("transaction_id", StringType(),
                expr="concat('TXN', lpad(cast(cast(floor(rand()*1000000000000) as bigint) as string), 12, '0'))")
    .withColumn("account_id", StringType(),
                expr="concat('ACC', lpad(cast(cast(floor(rand()*10000000000) as bigint) as string), 10, '0'))")
    .withColumn("transaction_date", DateType(),
                expr="date_add(to_date('2023-01-01'), cast(rand()*1095 as int))")
    .withColumn("transaction_time", StringType(),
                expr="concat(lpad(cast(cast(rand()*24 as int) as string),2,'0'),':',lpad(cast(cast(rand()*60 as int) as string),2,'0'),':',lpad(cast(cast(rand()*60 as int) as string),2,'0'))")
    .withColumn("transaction_type", StringType(),
                expr="CASE WHEN pmod(hash(id,'txn_type'),1000) < 80 THEN 'Deposit' WHEN pmod(hash(id,'txn_type'),1000) < 140 THEN 'Withdrawal' WHEN pmod(hash(id,'txn_type'),1000) < 190 THEN 'Transfer Out' WHEN pmod(hash(id,'txn_type'),1000) < 240 THEN 'Transfer In' WHEN pmod(hash(id,'txn_type'),1000) < 340 THEN 'Bill Payment' WHEN pmod(hash(id,'txn_type'),1000) < 420 THEN 'Direct Debit' WHEN pmod(hash(id,'txn_type'),1000) < 620 THEN 'Card Payment' WHEN pmod(hash(id,'txn_type'),1000) < 670 THEN 'ATM Withdrawal' WHEN pmod(hash(id,'txn_type'),1000) < 750 THEN 'Salary Credit' WHEN pmod(hash(id,'txn_type'),1000) < 780 THEN 'Refund' WHEN pmod(hash(id,'txn_type'),1000) < 900 THEN 'Purchase' WHEN pmod(hash(id,'txn_type'),1000) < 950 THEN 'Interest Credit' ELSE 'Fee' END")
    .withColumn("amount", DoubleType(), minValue=1.0, maxValue=10000.0)
    .withColumn("currency", StringType(), values=["USD"])
    .withColumn("is_credit", StringType(), expr="CASE WHEN rand() < 0.35 THEN 'Y' ELSE 'N' END")
    .withColumn("running_balance", DoubleType(), minValue=-1000.0, maxValue=100000.0)
    .withColumn("merchant_name", StringType(), values=merchants, percentNulls=0.4)
    .withColumn("merchant_category_code", StringType(),
                values=["5411", "5942", "5732", "4899", "5812", "5814", "4121", "5541", "5200", "4814"],
                percentNulls=0.4)
    .withColumn("channel", StringType(),
                expr="CASE WHEN pmod(hash(id,'channel'),1000) < 350 THEN 'Online Banking' WHEN pmod(hash(id,'channel'),1000) < 650 THEN 'Mobile App' WHEN pmod(hash(id,'channel'),1000) < 750 THEN 'ATM' WHEN pmod(hash(id,'channel'),1000) < 830 THEN 'Branch' WHEN pmod(hash(id,'channel'),1000) < 850 THEN 'Phone Banking' ELSE 'Direct Debit' END")
    .withColumn("branch_id", StringType(),
                expr="concat('BR', lpad(cast(cast(floor(rand()*100000) as int) as string), 5, '0'))", percentNulls=0.8)
    .withColumn("reference_number", StringType(),
                expr="upper(substr(sha2(cast(rand() as string), 256), 1, 16))")
    .withColumn("counterparty_account", StringType(),
                expr="concat('****', lpad(cast(cast(floor(rand()*10000) as int) as string), 4, '0'))", percentNulls=0.8)
    .withColumn("is_recurring", StringType(), expr="CASE WHEN rand() < 0.15 THEN 'Y' ELSE 'N' END")
    .withColumn("is_international", StringType(), expr="CASE WHEN rand() < 0.05 THEN 'Y' ELSE 'N' END")
    .withColumn("fraud_flag", StringType(),
                expr="CASE WHEN pmod(hash(id,'fraud'),1000) < 997 THEN 'N' WHEN pmod(hash(id,'fraud'),1000) < 999 THEN 'Suspected' ELSE 'Confirmed' END")
    .withColumn("status", StringType(),
                expr="CASE WHEN pmod(hash(id,'txn_status'),1000) < 960 THEN 'Completed' WHEN pmod(hash(id,'txn_status'),1000) < 980 THEN 'Pending' WHEN pmod(hash(id,'txn_status'),1000) < 990 THEN 'Failed' ELSE 'Reversed' END")
)

transactions_df = txn_gen.build()

transactions_df = transactions_df.withColumn(
    "description",
    F.when(F.col("merchant_name").isNotNull(),
           F.concat(F.col("transaction_type"), F.lit(" - "), F.col("merchant_name")))
    .otherwise(F.col("transaction_type"))
)

transactions_df.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/transactions")
print(f"Generated {transactions_df.count()} transactions")
transactions_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Output

# COMMAND ----------

import os

print(f"\nFiles in {VOLUME_PATH}:")
for d in sorted(os.listdir(VOLUME_PATH)):
    dir_path = os.path.join(VOLUME_PATH, d)
    if os.path.isdir(dir_path):
        csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
        total_size = sum(os.path.getsize(os.path.join(dir_path, f)) for f in os.listdir(dir_path))
        print(f"  {d}/ ({len(csv_files)} part files, {total_size / (1024*1024):.2f} MB)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Dataset | Rows | Description |
# MAGIC |---------|------|-------------|
# MAGIC | `branches/` | 200 | Branch/location reference data |
# MAGIC | `products/` | ~32 | Financial products catalog |
# MAGIC | `date_dimensions/` | ~1,096 | Date dimension table (3 years) |
# MAGIC | `customers/` | 50,000 | Customer master data with PII |
# MAGIC | `accounts/` | 75,000 | Account information |
# MAGIC | `transactions/` | 500,000 | Financial transactions |
# MAGIC
# MAGIC **PII Columns in customers:**
# MAGIC - `first_name`, `last_name`, `full_name` -- Personal names
# MAGIC - `email` -- Email addresses
# MAGIC - `phone` -- Phone numbers
# MAGIC - `date_of_birth` -- Date of birth
# MAGIC - `street_address`, `city`, `postal_code` -- Address information
# MAGIC - `annual_income` -- Financial information
# MAGIC - `credit_score` -- Credit information
# MAGIC
# MAGIC These PII columns can be used to demonstrate PII detection and masking with the Genie Code skills in this repo.
