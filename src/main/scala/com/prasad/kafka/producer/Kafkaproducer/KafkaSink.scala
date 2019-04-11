package com.prasad.kafka.producer.Kafkaproducer



import org.apache.spark.sql.ForeachWriter


class  KafkaSink extends ForeachWriter[( String)] {

  import java.util.Properties

  import org.apache.kafka.clients.producer._
  val  props = new Properties()
  val TOPIC="test"
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val results = new scala.collection.mutable.HashMap[String, String]
  var producer: KafkaProducer[String, String] = _

  def open(partitionId: Long,version: Long): Boolean = {
    print("open Connection")
    print(" PartitionId " +partitionId)
    print(" Version " +version)
    producer = new KafkaProducer(props)
    true
  }

  def process(value: ( String)): Unit = {
    println("Saving the record")
    producer.send(new ProducerRecord(TOPIC,  value))
  }

  def close(errorOrNull: Throwable): Unit = {
    println("closing the connection")
    producer.close()
  }
  //  for(i<- 1 to 10){
//    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
//    producer.send(record)
//  }
//
//  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
//  producer.send(record)
//
//  producer.close()
}
