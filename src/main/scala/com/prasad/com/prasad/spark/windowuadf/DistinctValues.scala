package com.prasad.com.prasad.spark.windowuadf

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType


class DistinctValues extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("_2", IntegerType) :: Nil)

  def bufferSchema: StructType = StructType(StructField("values", MapType(StringType, LongType))::Nil)

  def dataType: DataType =  MapType(StringType, LongType)
  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map()
  }

  def update(buffer: MutableAggregationBuffer, input: Row) : Unit = {
    val str = input.getAs[String](0)
    var mp = buffer.getAs[Map[String, Long]](0)
    var c:Long = mp.getOrElse(str, 0)
    c = c + 1
    mp = mp  + (str -> c)
    buffer(0) = mp
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) : Unit = {


    var mp1 = buffer1.getAs[Map[String, Long]](0)
    var mp2 = buffer2.getAs[Map[String, Long]](0)
    mp2 foreach {
      case (k ,v) => {
        var c:Long = mp1.getOrElse(k, 0)
        c = c + v
        mp1 = mp1 + (k -> c)
      }
    }
    buffer1(0) = mp1
  }

  def evaluate(buffer: Row): Any = {
    buffer.getAs[Map[String, LongType]](0)
  }
}