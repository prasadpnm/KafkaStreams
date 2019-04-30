package com.prasad.com.prasad.spark.windowuadf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CustomUDAFWindows extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("val1", DoubleType) ::
      Nil)

  // This is the internal fields you keep for computing your aggregate.
  case class inputc(val1:DoubleType)
  override def bufferSchema: StructType = StructType(
      StructField("agg1", DoubleType) ::
      StructField("agg2", DoubleType) ::Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("success", DoubleType) ::
      StructField("failure", DoubleType) ::
      Nil)
  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0

  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if(input.getAs[Double](0)/100==2||input.getAs[Double](0)/100==3){
      buffer(0) = buffer.getAs[Double](0) + 1
    }
    if(input.getAs[Double](0)/100==4||input.getAs[Double](0)/100==5){
      buffer(1) = buffer.getAs[Double](1) + 1
    }

  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row)= {
    (buffer.getDouble(0),buffer.getDouble(1))
  }
}