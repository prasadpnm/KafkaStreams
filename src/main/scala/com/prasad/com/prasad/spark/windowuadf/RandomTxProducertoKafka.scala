package com.prasad.com.prasad.spark.windowuadf

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object RandomTxProducertoKafka{

  def main(args: Array[String]) {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val accountNumbers = List("ABC001", "ABC002", "ABC003", "ABC004")
    val txTypes = List("Food","Taxi","AirTravel","Office Supplies","Others")
    val transactionAmounts = List(200, 201, 300, 301, 400, 404, 500, 505)

    val producer = new KafkaProducer[Nothing, String](props)


    System.out.print(">>>Publishing Random Transactions to 'test' topic")
    val topic = args.lift(1).getOrElse("test")

    var currentStep = 0
    while(true){

     Thread.sleep(1000)
      val accountNumber =  accountNumbers(scala.util.Random.nextInt(accountNumbers.size))
      val txtype = txTypes(scala.util.Random.nextInt(txTypes.size))
      val currentDate = (new java.text.SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")).format(new java.util.Date())
      val txAmount =  transactionAmounts(scala.util.Random.nextInt(transactionAmounts.size))
      val transactionLogLine = s"$currentStep,$currentDate,$accountNumber,$txAmount,$txtype"
      producer.send(new ProducerRecord(topic, transactionLogLine))
      println("Sent -> " + transactionLogLine)
      currentStep = currentStep + 1
    }

    producer.close()
  }
}

