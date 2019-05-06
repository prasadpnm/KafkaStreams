package com.prasad.com.prasad.spark.windowuadf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DoubleType, LongType,  StringType, StructField, StructType}
import org.apache.spark.sql.types.MapType

case class complex(a:Double,b:Double)
class GroupByTxTypeUDAF extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("txAmount", DoubleType) ::
      StructField("txType", StringType) ::

      Nil)

  // This is the internal fields you keep for computing your aggregate.
  case class inputc(val1:DoubleType)
  override def bufferSchema: StructType = StructType(StructField("values", MapType(StringType, MapType(StringType, LongType)))::Nil)


  // This is the output type of your aggregatation function.
  override def dataType = MapType(StringType, MapType(StringType, LongType))

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) =Map[String,Map[String,Long]]()


  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {


    var key=input.getAs[String](1)
    var value=input.getAs[Double](0)
    var mpAccum=buffer.getAs[Map[String,Map[String,Long]]](0)

    var c:Map[String,Long] = mpAccum.getOrElse(key, Map[String,Long]())
    var ckey=""
    var cval=0L
    if(value>450){
      ckey="Large"
      cval=1L +c.getOrElse(ckey,0L)
    }else if( value> 300){
      ckey="Average"
      cval=1L +c.getOrElse(ckey,0L)
    } else{
      ckey="Low"
      cval=1L +c.getOrElse(ckey,0L)
    }

    c = c + (ckey ->cval)
    mpAccum = mpAccum  + (key -> c)
    buffer(0) = mpAccum



  }


//  mp2 foreach {
//    case (k ,v) => {
//      var c:Double = mp1.getOrElse(k, 0)
//      c = c + v
//      mp1 = mp1 + (k -> c)
//    }
//  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var mp1 = buffer1.getAs[Map[String, Map[String,Long]]](0)
    var mp2 = buffer2.getAs[Map[String, Map[String,Long]]](0)
    mp2 foreach {
      case (k ,v) => {
        var c: Map[String, Long] = mp1.getOrElse(k, Map[String,Long]())
        v foreach {
          case (k1,v1) =>{
            var c1:Long=c.getOrElse(k1,0L)
           val total= c1+v1
           c=c+(k1->total)
          }
            mp1 = mp1 + (k -> c)
      }
        mp1 = mp1 + (k -> c)
      }
    }
    buffer1(0) = mp1
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row)= {
    buffer.getAs[Map[String,Map[String,Long]]](0)
  }
}