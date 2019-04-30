package com.prasad.com.prasad.spark.windowuadf

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaIntegrationWindowingExample {

  case class Events(id:Integer,timestamp:TimeStamp, eventid:String,status:Integer)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark-window-Integration")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("group.id","test1")
     // .option()
      .load()

   val valuedf= df.selectExpr("CAST(value AS STRING)")//.map(convertStringToEvents)

   val test= df.withColumn("_tmp", split($"value", "\\,")).select(
      $"_tmp".getItem(0).as("seqid"),
     to_timestamp($"_tmp".getItem(1), "MM/dd/yyyy' 'HH:mm:ss")
      .as("timestamp"),
      $"_tmp".getItem(2).as("eventid"),
      $"_tmp".getItem(3).as("statuscode")
    ).drop("_tmp")
//.getField("start").as("caluclatedTime")
   // var test2=test.withColumn("timestamp",ts).drop("timestamps")
   var test2= test.withWatermark("timestamp","30 seconds")
        .groupBy(
          window($"timestamp","10 seconds").getField("start").as("caluclatedtime"),
          $"eventid"
        ).agg(count(when($"statuscode" like "2%" ,1)).as("success"),
          count(when($"statuscode" like "4%",1) ).as("Failure")
        )
  // val test= valuedf.map(convertStringToEvents).as[Events]

   // val df3=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    test2.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate","false")
      .start()
     // .awaitTermination()

      val df2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      .option("group.id","test2")
      // .option()
      .load()

    val valuedf2= df2.selectExpr("CAST(value AS STRING)")//.map(convertStringToEvents)

    val testv2= df2.withColumn("_tmp", split($"value", "\\,")).select(
      $"_tmp".getItem(0).as("seqid"),
      to_timestamp($"_tmp".getItem(1), "MM/dd/yyyy' 'HH:mm:ss")
        .as("timestamp"),
      $"_tmp".getItem(2).as("eventid"),
      $"_tmp".getItem(3).as("statuscode")
    ).drop("_tmp")

    val gm = new CustomUDAFWindows



    var testv22= testv2.withWatermark("timestamp","30 seconds")
      .groupBy(
        window($"timestamp","10 seconds").getItem("start") as "caluclatedtime",
        $"eventid"
      ).agg(gm(col("statuscode"))
    )

    //val testdf=testv22.select("eventid","caluclatedtime","aggregation.*")
    testv22.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate","false")
      .start()
      .awaitTermination()


//    def stringifyRows(df: DataFrame, sep: String): DataFrame
//    = df.map(row => row.mkString(sep)).toDF()

  }

}
