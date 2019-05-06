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
      $"_tmp".getItem(3).as("amount"),
      $"_tmp".getItem(4).as("txType")
    ).drop("_tmp")

    val gm = new GroupByTxTypeUDAF
    val tx= new TxTotalUDAF



    var testv22= testv2.withWatermark("timestamp","10 minutes")
      .groupBy(
        window($"timestamp","5 minute").getItem("start") as "caluclatedtime",
        $"eventid"
      ).agg(gm(col("amount"),col("txType")) as "txByCounts", tx(col("amount")) as "totalAmount")


    testv22.printSchema()

    //val testdf=testv22.select("eventid","caluclatedtime","aggregation.*")
    testv22.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate","false")
      .start()
      .awaitTermination()


//    def stringifyRows(df: DataFrame, sep: String): DataFrame
//    = df.map(row => row.mkString(sep)).toDF()

  }

}
