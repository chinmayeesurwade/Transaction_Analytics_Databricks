# Databricks notebook source
# Independent portfolio project with synthetic data. Upload the two CSVs as tables first.
# COMMAND ----------
from pyspark.sql import functions as F, Window
raw_t = spark.table("portfolio_transactions_raw")
raw_c = spark.table("portfolio_customers_raw")
print("raw transactions", raw_t.count(), "customers", raw_c.count())

# COMMAND ----------
customers = raw_c.select("customer_id", "segment", "region", F.to_date("join_date").alias("join_date"))
typed = raw_t.select("transaction_id", "customer_id", F.to_timestamp("transaction_ts").alias("transaction_ts"),
                     "channel", "status", F.col("amount_inr").cast("double").alias("amount_inr"),
                     F.col("processing_seconds").cast("integer").alias("processing_seconds"))
first = Window.partitionBy("transaction_id").orderBy("transaction_ts")
ranked = typed.withColumn("duplicate_rank", F.row_number().over(first))
joined = ranked.join(customers.select("customer_id").withColumn("known_customer",F.lit(1)),"customer_id","left")
validated = joined.withColumn("reject_reason",F.concat_ws("; ",
    F.when(F.col("duplicate_rank")>1,"duplicate transaction ID"),
    F.when(F.col("known_customer").isNull(),"unknown customer"),
    F.when(F.col("amount_inr")<=0,"nonpositive amount"),
    F.when(F.col("channel").isNull() | (F.trim("channel")==""),"missing channel")))
clean = validated.filter(F.col("reject_reason")=="").drop("duplicate_rank","known_customer","reject_reason")
rejected = validated.filter(F.col("reject_reason")!="").select("transaction_id","reject_reason")
print("clean",clean.count(),"rejected",rejected.count())
assert raw_t.count()==clean.count()+rejected.count()
assert clean.count()==clean.select("transaction_id").distinct().count()

# COMMAND ----------
clean.write.mode("overwrite").format("delta").saveAsTable("portfolio_transactions_clean")
customers.write.mode("overwrite").format("delta").saveAsTable("portfolio_customers")
mart=(clean.join(customers,"customer_id")
      .withColumn("month",F.date_format("transaction_ts","yyyy-MM"))
      .groupBy("month","channel","segment")
      .agg(F.count("*").alias("transaction_count"),
           F.sum(F.when(F.col("status")=="Failed",1).otherwise(0)).alias("failed_count"),
           F.round(F.sum(F.when(F.col("status")=="Completed",F.col("amount_inr")).otherwise(0)),2).alias("completed_value_inr"),
           F.round(F.avg(F.when(F.col("status")=="Completed",F.col("processing_seconds"))),1).alias("avg_completed_seconds"))
      .withColumn("failure_rate_pct",F.round(100*F.col("failed_count")/F.col("transaction_count"),2)))
mart.write.mode("overwrite").format("delta").saveAsTable("portfolio_monthly_channel_segment")
assert mart.agg(F.sum("transaction_count")).first()[0]==clean.count()
display(mart.orderBy("month","channel","segment"))

# COMMAND ----------
print("raw",raw_t.count(),"clean",clean.count(),"rejected",rejected.count(),"mart rows",mart.count())
display(rejected.groupBy("reject_reason").count())
