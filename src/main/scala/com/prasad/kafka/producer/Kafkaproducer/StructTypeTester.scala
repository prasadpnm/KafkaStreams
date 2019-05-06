package com.prasad.kafka.producer.Kafkaproducer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection


object StructTypeTester {
  case class Complex(a: Double, b: Double)
  case class Gummy(p: Integer, q: Integer)
  case class Data(id: Integer, dummy: Complex, amt: Double, gummy: Gummy)

  def main(args: Array[String]): Unit = {

//    case class A(key: String, time: java.sql.Timestamp, date: java.sql.Date, decimal: java.math.BigDecimal, map: Map[String, Int], nested: Seq[Map[String, Seq[Int]]])
//    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
//    ScalaReflection.sch
//    schema.printTreeString



    val rowSeq = Seq(
      Row(1, Row("a", "b"), 40.00, Row(1,2)),
      Row(2, Row("c", "d"), 50.00, Row(3,4))
    )



    val schema = ScalaReflection.schemaFor[Complex].dataType.asInstanceOf[StructType]
    print(schema)
  }



}
