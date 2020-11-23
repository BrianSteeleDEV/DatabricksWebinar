// Databricks notebook source
// MAGIC %md
// MAGIC Structure Streaming Basic (Event Hubs)
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

// DBTITLE 1,Basic Azure Event Hub Streaming
val schema =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val customEventhubParameters =
  EventHubsConf(EventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(customEventhubParameters.toMap)
  .load()
  //Transformations
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("enqueuedTimeTmp", from_utc_timestamp($"enqueuedTime", "America/New_York"))
  .withColumn("dateColumn", $"enqueuedTime".cast(DateType))
  .withColumn("data", from_json($"json", schema))
  .drop("enqueuedTime")
  .withColumnRenamed("bodyTmp","body")
  .withColumnRenamed("enqueuedTimeTmp","enqueuedTime")
  //Output 
  .writeStream
  .format("delta")
  .partitionBy("dateColumn")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/test/transactions_basic/_checkpoints/etl-from-eventhub")
  .option("path", "/mnt/test/transactions_basic")
  .start()

// COMMAND ----------

// DBTITLE 1,Azure Event Hub Stream
val schema =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val customEventhubParameters =
  EventHubsConf(EventHubConnectionString)
  .setStartingPosition(org.apache.spark.eventhubs.EventPosition.fromEndOfStream)
  .setMaxEventsPerTrigger(100)

val dfEventHub = spark
  //Input/Source Parameters
  .readStream
  .format("eventhubs")
  .options(customEventhubParameters.toMap)
  .load()


//Transformations
val dfTransform = dfEventHub
  .withColumn("json", expr("cast(body as string)"))
  .withColumn("enqueuedTimeTmp", from_utc_timestamp($"enqueuedTime", "America/New_York"))
  .withColumn("dateColumn", $"enqueuedTime".cast(DateType))
  .withColumn("data", from_json($"json", schema))
  .drop("enqueuedTime")
  .withColumnRenamed("bodyTmp","body")
  .withColumnRenamed("enqueuedTimeTmp","enqueuedTime")
  //Output 

dfTransform
  .writeStream
  .format("delta")
  .partitionBy("dateColumn")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/test/transactions_basic2/_checkpoints/etl-from-eventhub") //Note using a new checkpoint folder
  .option("path", "/mnt/test/transactions_basic2")
  .start()

// COMMAND ----------

// DBTITLE 1,Aggregation
val schema =  spark.read.option("multiline","true").json("/mnt/test/transction.json").schema

val customEventhubParameters =
  EventHubsConf(EventHubConnectionString)
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
  .groupBy($"data.transaction.state")
  .agg(sum($"data.transaction.totalsaleamount").alias("total sales"))

display(dfEventHubs)

