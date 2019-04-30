package com.prasad.spark.udaf.distinct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CustomUDAFTester {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-UDAFTester")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val ids = spark.range(1, 20)
    var idsmap = ids.map(t => (t, t + 1))
    val idsmap2 = idsmap.withColumn("val1", (idsmap("_1") % 3))
    val idsmap3 = idsmap2.withColumn("val2", (idsmap("_1") % 3)).withColumn("group_id", idsmap("_1"))
    val finalmap = idsmap3.drop("_1").drop("_2")
    finalmap.printSchema()
    finalmap.show()

    val gm = new CustomUDAF

    var aggval = finalmap.groupBy("group_id").agg(gm(col("val1"), col("val2")) as "computution" )
   val finaldf= aggval.select(aggval.col("group_id"), aggval.col("computution"))
    finaldf.printSchema()
    val testdf=finaldf.select("computution.*","group_id")
    testdf.printSchema()
    testdf.show()
   // finaldf.select(children("computution", finaldf): _*).withColumn("group_id",finaldf("group_id")).show()
  //  tt.printSchema()
    //tt.show()
//    def children(colname: String, df: DataFrame) = {
//      val parent = df.schema.fields.filter(_.name == colname).head
//      val fields = parent.dataType match {
//        case x: StructType => x.fields
//        case _ => Array.empty[StructField]
//      }
//      fields.map(x => col(s"$colname.${x.name}"))
//    }

  }

}
