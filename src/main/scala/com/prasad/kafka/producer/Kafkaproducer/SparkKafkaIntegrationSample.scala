package com.prasad.kafka.producer.Kafkaproducer


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object SparkKafkaIntegrationSample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
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
      .load()
    val df3=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //    df.writeStream
    //      .format("console")
    //      .option("truncate","false")
    //      .start()
    df.printSchema()
    val filter= df3.filter(!$"value".like("updatedo%"))
    val df2 = filter.map(teenager => "updatedo: " + teenager.getAs[String]("value"))

   //df2.filter($"value".like("updated%"))

    val sink:KafkaSink=new KafkaSink()

    val query =
      df2
        .writeStream
        .foreach(sink)
        .outputMode("update")
       // .trigger(ProcessingTime("25 seconds"))
        .start()
  .awaitTermination()

//    df2.writeStream
//              .format("kafka")
//            .option("kafka.bootstrap.servers", "localhost:9092")
//            .option("topic", "test")
//            .option("checkpointLocation", "/Users/prasad/Documents/test")
//            .outputMode("update")
//            .start()
//            .awaitTermination()



  }
}
