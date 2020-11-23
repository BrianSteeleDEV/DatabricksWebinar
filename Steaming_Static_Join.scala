// Databricks notebook source
// MAGIC %md
// MAGIC Structure Streaming Stream to Static Join
// MAGIC -----------------------------------
// MAGIC 
// MAGIC Notes: Streaming of Event Hub messages to Raw Data Storage (RDS) as JSON messages. Requires com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.6. library

// COMMAND ----------

val EventHubConnectionString = "Endpoint=sb://databrickswebinar.servicebus.windows.net/;SharedAccessKeyName=listener;SharedAccessKey=WMHtd96VwVKnAlLyKy7YSeg6JRvfjlwjBWgTZw4G1lw=;EntityPath=transaction"

// COMMAND ----------

import com.microsoft.azure.eventhubs._  //NOTE - must have this for event hub streaming
import org.apache.spark.eventhubs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.delta.tables._
import functions._

// COMMAND ----------

// DBTITLE 1,Load Static Data Set (Territories) with Inner Join
//Load Static File
val dfTerritories = spark.read
.format("com.databricks.spark.csv")
.option("header", "true")
.option("inferSchema", "true")
.option("delimiter", "|")
.load("/mnt/test/SaleTerritories.txt")

val schema =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val customEventhubParameters = EventHubsConf(EventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

val dfEventHubs = spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(customEventhubParameters.toMap)
  .load()
  //Transformations
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("enqueuedTimeTmp", from_utc_timestamp($"enqueuedTime", "America/New_York"))
  .withColumn("data", from_json($"json", schema))
  .drop("enqueuedTime")
  .withColumnRenamed("enqueuedTimeTmp","enqueuedTime")
  //Output 


val dfJoinToStatic = dfEventHubs.alias("transactions")
  .join(dfTerritories.alias("territory"), expr("territory.state = transactions.data.transaction.state"))
  .groupBy($"territory.territory")
  .agg(sum($"transactions.data.transaction.totalsaleamount").alias("total sales"))
  .select(
  $"territory.territory".as("territory"),
  $"total sales")

display(dfJoinToStatic)

// COMMAND ----------

// DBTITLE 1,Load Static Data Set (Territories) with Left Join
//Load Static File
val dfTerritories = spark.read
.format("com.databricks.spark.csv")
.option("header", "true")
.option("inferSchema", "true")
.option("delimiter", "|")
.load("/mnt/test/SaleTerritoriesMissing.txt")

val schema =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val customEventhubParameters = EventHubsConf(EventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

val dfEventHubs = spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(customEventhubParameters.toMap)
  .load()
  //Transformations
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("enqueuedTimeTmp", from_utc_timestamp($"enqueuedTime", "America/New_York"))
  .withColumn("data", from_json($"json", schema))
  .drop("enqueuedTime")
  .withColumnRenamed("enqueuedTimeTmp","enqueuedTime")
  //Output 


val dfJoinToStatic = dfEventHubs.alias("transactions")
  .join(dfTerritories.alias("territory"), expr("territory.state = transactions.data.transaction.state"), "left")
  .groupBy($"territory.territory")
  .agg(sum($"transactions.data.transaction.totalsaleamount").alias("total sales"))
  .select(
  expr("Case When territory.territory is null then 'No Territory' else territory.territory End").as("territory"),
  $"total sales")

display(dfJoinToStatic)
