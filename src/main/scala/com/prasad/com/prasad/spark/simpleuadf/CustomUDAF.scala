package com.prasad.com.prasad.spark.simpleuadf


import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CustomUDAF extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  case class input(val1:Double,val2:Double)
  case class buffer(count:Long,product:Double,sum:Double)
  case class temp(custm1:String,custom2:String)
  case class result(resultu:Double,value2u:Double,value3u:Double,composite:temp)

  override def inputSchema: org.apache.spark.sql.types.StructType =ScalaReflection.schemaFor[input].dataType.asInstanceOf[StructType]

//    StructType(StructField("val1", DoubleType) ::
//      StructField("val2", DoubleType) ::
//      Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType =ScalaReflection.schemaFor[buffer].dataType.asInstanceOf[StructType]


//    StructType(
//    StructField("count", LongType) ::
//      StructField("product", DoubleType) ::
//      StructField("sum", DoubleType) ::Nil
//  )

  // This is the output type of your aggregatation function.
  override def dataType: org.apache.spark.sql.types.StructType =ScalaReflection.schemaFor[result].dataType.asInstanceOf[StructType]
//    StructType(StructField("result", DoubleType) ::
//      StructField("value2", DoubleType) ::
//      StructField("value3", DoubleType) ::
//      Nil)
  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
    buffer(2) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
    buffer(2) = buffer.getAs[Double](2) + input.getAs[Double](1)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row)= {
    //(math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0)),buffer.getDouble(2),0.0)
    result(math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0)),buffer.getDouble(2),0.0,temp("test","test"))
  }
}