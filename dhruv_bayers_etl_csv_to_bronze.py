# Databricks notebook source
# MAGIC %md
# MAGIC {"payment_url" : "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardPayment.csv",
# MAGIC "card_payment_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardPayment.csv",
# MAGIC "card_refund_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardRefund.csv",
# MAGIC "customer_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/customer.csv",
# MAGIC "product_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/Product.csv",
# MAGIC "product_incr_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/Product_Incr.csv",
# MAGIC "return_order_url" :/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/ReturnOrder.csv",
# MAGIC "sales_order_url":"/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/SalesOrder.csv"
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Import libraries
import json
import time
import re
import logging
import traceback

from pyspark.sql.functions import col, regexp_extract, length, col, try_to_date, year, quarter

import plotly.graph_objects as go
import plotly.express as px

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

# DBTITLE 1,Processing the variables
dbutils.widgets.text(
    "params_json",
    '{"payment_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardPayment.csv", "card_payment_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardPayment.csv", "card_refund_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/CardRefund.csv", "customer_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/customer.csv", "product_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/Product.csv", "product_incr_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/Product_Incr.csv", "return_order_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/ReturnOrder.csv", "sales_order_url": "/Workspace/Shared/DataHackathon28NOV2025/HackathonSourceFiles/SalesOrder.csv"}',
    "Parameters"
)
params_json_str = dbutils.widgets.get("params_json")
try:
    params = json.loads(params_json_str) if params_json_str.strip() else {}
except json.JSONDecodeError:
    params = {}
for key, value in params.items():
    globals()[key] = value
    print(key, value)

# COMMAND ----------

# DBTITLE 1,Utlity functions
# define to read bronze tables into dataframes

def ingest_raw_tables(table,url):
    logger.info(f"Reading {i} table")
    df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(url)
    df.write.mode("overwrite").saveAsTable(f"dhruv_cdl_{table}") 

def fetch_raw_tables(table,url):
    logger.info(f"Reading {i} table")
    df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(url)

    return df
    #df.write.mode("overwrite").saveAsTable(f"dhruv_cdl_{table}")       
    #df.write.format("delta").mode("overwrite").option("path", f"/Workspace/Repos/dhruvpokhariyal007@gmail.com/#bayers_hackathon_2025/bronze_layer/{table}").saveAsTable(f"dhruv_bronze_{table}")

#Creating silver delta tables.
def ingest_silver_tables(table,df):
    logger.info(f"Writing table: {table} ")
    df.write.mode("overwrite").format("delta").option("mergeSchema", True).saveAsTable(f"dhruv_sem_{table}")      


def ingest_gold_tables(table,df):
    logger.info(f"Writing table: {table} ")
    
    df.write.mode("overwrite").format("delta").option("mergeSchema", True).saveAsTable(f"dhruv_gold_{table}")           

# Define a function to sanitize column names
def sanitize_column(name):
    return re.sub(r'[^a-zA-Z0-9]', '_', name)


# COMMAND ----------

# DBTITLE 1,Ingesting tables
bronze_tables = ["payment", "card_payment", "card_refund", "customer", "product", "return_order", "sales_order"]

incremental_tables = ["product_incr"]
Email_regex = r'[]'# record becoms zero due to space in first and las t name of emails.
mandatory_attributes = {
"customer": ["Customer Id","Email", "Country"], 
}
try:
    for i in bronze_tables:
        print("tables", i)
        #ingest_raw_tables(i, eval(f"{i}_url"))

        df = fetch_raw_tables(i, eval(f"{i}_url"))
        # Transform and check for the validation here only CDL and then create the silver layer table directly.

        if df is None:
            logger.warning(f"{i} table returned None, skipping.")
            continue

        # Dublicates handled
        if df.count()>0:
            print("dropping duplicates")
            df = df.dropDuplicates()
            # null

        # Null checks for mandatory attributes
        if i in mandatory_attributes:
            for col_name in mandatory_attributes[i]:
                if col_name in df.columns:
                    df = df.filter(col(col_name).isNotNull())

        # email checks for bayers if we had that column.
        if "Email" in df.columns:      
            print("email validations")  
            df = df.filter(col("email").rlike("@"))   
            #phone_pattern = r"^d{3}-\d{4}$"  # For format XXX-XXX-XXXX
        
        # Apply to all column names
        sanitized_cols = [sanitize_column(col) for col in df.columns]
        df_clean = df.toDF(*sanitized_cols)   

        # Creating cleaned silver tables as delta tables.
        ingest_silver_tables(i,df_clean)
        print("Created table %s",i)         
        logger.info(f"ingested {i} table")

    # second job for incremental loads and SCD for gold tables

except Exception as e:
    logger.error("internal server error%s",str(e))

# COMMAND ----------

# DBTITLE 1,Ingesting to the gold layers


# COMMAND ----------

# DBTITLE 1,5th KPI's
# sales table
df = spark.table("workspace.default.dhruv_sem_sales_order")
total = df.count()
print('total', total)

cash = df.filter(col("Mode_of_Payment") == 'Cash').count()
digital = df.filter(col("Mode_of_Payment") == 'Googlepay').count()
paytm = df.filter(col("Mode_of_Payment") == 'Paytm').count()
card = df.filter(col("Mode_of_Payment") == 'Card').count()


digital_pct = round((digital/total) * 100, 2) if total > 0 else 0
print(f" Cash: {cash}")
print(f" digital: {digital_pct}")
print(f" Card: {card}")
print(f" Paytm: {paytm}")

fig = go.Figure(
    go.Indicator(
        mode="number",
        value=digital_pct,
        title={"text": "Digital Payments (%)"},
        number={"suffix": "%", "valueformat": ".2f"}  
    )
)
fig.show()

# COMMAND ----------

# DBTITLE 1,Sales QoQ
sales_df = spark.table("workspace.default.dhruv_sem_sales_order")
card_payment_df = spark.table("workspace.default.dhruv_sem_card_payment")

sales_df = sales_df.withColumn("creationDate_parsed",
    try_to_date(col("creationDate"), "d/M/yyyy"))

sales_with_quarter = sales_df.withColumn("sales_year", year(col("creationDate_parsed"))).withColumn("sales_quarter", quarter(col("creationDate_parsed")))

# Join sales with card payment to get Amount
joined_df = sales_with_quarter.join(
    card_payment_df,
    sales_with_quarter["SalesOrderId"] == card_payment_df["SalesOrderId"],
    "left"
)

# Add null check for Amount
joined_df = joined_df.filter(col("Amount").isNotNull())

# Aggregate total Amount per year and quarter
qoq_sales = (
    joined_df.groupBy("sales_year", "sales_quarter")
    .agg({"Amount": "sum"})
    .orderBy("sales_year", "sales_quarter")
    .withColumnRenamed("sum(Amount)", "total_amount")
    .filter(col("total_amount").isNotNull())
    .filter(col("sales_year").isNotNull())
    .filter(col("sales_quarter").isNotNull())
)

display(qoq_sales)

# COMMAND ----------

# DBTITLE 1,Incremental load for products tables
delta_table.alias("target").merge(
    staging_df.alias("source"),
    "target.id = source.id AND target.is_current = 'Y'"
).whenMatchedUpdate(set={
    "is_current": lit("N"),
    "end_date": current_timestamp()
}).whenNotMatchedInsert(values={
    "id": col("source.id"),
    "name": col("source.name"),
    "city": col("source.city"),
    "is_current": lit("Y"),
    "start_date": col("source.start_date"),
    "end_date": lit("9999-12-31").cast("timestamp")
}).execute()