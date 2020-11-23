// Databricks notebook source
// MAGIC %md
// MAGIC Structure Streaming with foreachBatch (Event Hubs)
// MAGIC -----------------------------------
// MAGIC 
// MAGIC Notes: Streaming of Event Hub messages to Raw Data Storage (RDS) as JSON messages. Requires com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.6. library

// COMMAND ----------

val TransactionEventHubConnectionString = "Endpoint=sb://databrickswebinar.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=WMHtd96VwVKnAlLyKy7YSeg6JRvfjlwjBWgTZw4G1lw=;EntityPath=transaction"

// COMMAND ----------

import com.microsoft.azure.eventhubs._  //NOTE - must have this for event hub streaming
import org.apache.spark.eventhubs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.delta.tables._
import functions._

spark.conf.set("spark.sql.shuffle.partitions", "1")

// COMMAND ----------

//Transactions
val schematrans =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val TransactioncustomEventhubParameters = EventHubsConf(TransactionEventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

val dfTransactions = spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(TransactioncustomEventhubParameters.toMap)
  .load()
  //Transformations
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("data", from_json($"json", schematrans))
  .withColumn("customerid", $"data.transaction.customerid")
  .withColumn("itemid", $"data.transaction.itemid")
  .withColumn("transactionid", $"data.transaction.transactionid")
  .select(
    $"customerid",
    $"itemid",
    $"enqueuedTime".as("transactiontime"),
    $"transactionid")



// COMMAND ----------

dfTransactions
.writeStream
.format("delta")
.option("checkpointLocation", "/mnt/test/transaction_batch/_checkpoints")
.foreachBatch
  { (batchDF: DataFrame, batchId: Long) =>

    val dflasttransaction = batchDF
      .groupBy("customerid")
      .agg(max($"transactiontime").alias("last_transaction"))

    val dtcustomer_last_transaction = DeltaTable.forPath("/mnt/test/customer_last_transaction")

    dtcustomer_last_transaction.as("c")
      .merge(
        dflasttransaction.as("n"), 
        "c.customerid = n.customerid")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()

    val dttransaction_batch = DeltaTable.forPath("/mnt/test/transaction_batch")
    
    dttransaction_batch.as("c")
      .merge(
        batchDF.as("n"), 
        "c.transactionid = n.transactionid")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
  }
.outputMode("append")
.start()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from test.transaction_batch

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from test.customer_last_transaction

// COMMAND ----------

// MAGIC %sql
// MAGIC create database test 
