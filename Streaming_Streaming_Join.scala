// Databricks notebook source
// MAGIC %md
// MAGIC Structure Streaming Stream to Stream Join (Event Hubs)
// MAGIC -----------------------------------
// MAGIC 
// MAGIC Notes: Streaming of Event Hub messages to Raw Data Storage (RDS) as JSON messages. Requires com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.6. library

// COMMAND ----------

val TransactionEventHubConnectionString = "Endpoint=sb://databrickswebinar.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=WMHtd96VwVKnAlLyKy7YSeg6JRvfjlwjBWgTZw4G1lw=;EntityPath=transaction"
val ViewEventHubConnectionString = "Endpoint=sb://databrickswebinar.servicebus.windows.net/;SharedAccessKeyName=Listener;SharedAccessKey=IkrSrhgJw+v/3aYCMArZgTkAROBN3ggf5sL/4VBLuaI=;EntityPath=itemview"

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

//Load Static File
val dfTerritories = spark.read
.format("com.databricks.spark.csv")
.option("header", "true")
.option("inferSchema", "true")
.option("delimiter", "|")
.load("/mnt/test/SaleTerritories.txt")

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
  .withColumn("timestamp", current_timestamp())



// COMMAND ----------

//Views
val schemaview =  spark.read.option("multiline","true").json("/mnt/test/itemview.json").schema

val ViewcustomEventhubParameters = EventHubsConf(ViewEventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

val dfViews = spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(ViewcustomEventhubParameters.toMap)
  .load()
  //Transformations
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("data", from_json($"json", schemaview))
  .withColumn("customerid", $"data.itemview.customerid")
  .withColumn("itemid", $"data.itemview.itemid")
  .withColumn("timestamp", current_timestamp())

// COMMAND ----------

//Join
val dfInnerJoinStreaming = dfTransactions.alias("trans").withWatermark("enqueuedTime","30 seconds")  
  .join(dfViews.alias("view").withWatermark("enqueuedTime","30 seconds"), 
        expr("trans.customerid = view.customerid and trans.itemid <> view.itemid and view.enqueuedTime >= trans.enqueuedTime and view.enqueuedTime <= trans.enqueuedTime + interval 30 seconds"))
  .join(dfTerritories.alias("territory"), expr("territory.state = trans.data.transaction.state"))  //Static Table
  .select(
  $"territory.territory".as("territory"),
  $"trans.customerid".as("customerid"),
  $"trans.itemid".as("transaction_item"), 
  $"view.itemid".as("view_item"),  
  $"view.enqueuedTime".as("view_time"),
  $"trans.enqueuedTime".as("trans_time"))

display(dfInnerJoinStreaming)

// COMMAND ----------

//Left Join
val dfJoinStreaming = dfTransactions.alias("trans").withWatermark("enqueuedTime","30 seconds")  
  .join(dfViews.alias("view").withWatermark("enqueuedTime","30 seconds"), 
        expr("trans.customerid = view.customerid and trans.itemid <> view.itemid and view.enqueuedTime >= trans.enqueuedTime and view.enqueuedTime <= trans.enqueuedTime + interval 30 seconds"), "leftOuter")
  .join(dfTerritories.alias("territory"), expr("territory.state = trans.data.transaction.state"))  //Static Table
  .select(
  $"territory.territory".as("territory"),
  $"trans.customerid".as("customerid"),
  $"trans.itemid".as("transaction_item"), 
  $"view.itemid".as("view_item"),  
  $"view.enqueuedTime".as("view_time"),
  $"trans.enqueuedTime".as("trans_time"))

// COMMAND ----------

display(dfJoinStreaming)
