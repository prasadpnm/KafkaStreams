package com.prasad.com.prasad.spark.windowuadf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class TxTotalUDAF  extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("val1", LongType) ::
      Nil)

  // This is the internal fields you keep for computing your aggregate.
  case class inputc(val1:DoubleType)
  override def bufferSchema: StructType = StructType(
    StructField("agg1", LongType) ::Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType=LongType

  override def deterministic: Boolean = true
  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L


  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {


      buffer(0) = buffer.getAs[Long](0) + input.getAs[Long](0)


  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)

  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row)= {
    (buffer.getAs[Long](0))
  }

}
