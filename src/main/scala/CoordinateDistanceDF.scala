import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim, udf, sum}

import scala.math._

/**
  * Created by Administrator on 2017-05-23.
  */
object CoordinateDistanceDF {
  var R = 6372800 //meter
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("DF_CoordinateDistance").config("spark.sql.broadcastTimeout", 36000).getOrCreate()

    val defaultDf = spark.read.option("header", "false").option("delimiter","|").csv(args(0)).distinct().select(col("*")).na.fill("")
    val sideDf = spark.read.option("header", "false").option("delimiter","|").csv(args(1)).distinct().na.fill("")
      .select(col("_c2"), col("_c4"), col("_c5"), col("_c6"), col("_c8"), col("_c13"), trim(col("_c28")).alias("lat2"), trim(col("_c29")).alias("lon2"), col("_c36")).limit(10000)
    //    df.select(df.columns(2), df.columns(4), df.columns(6), df.columns(8), df.columns(13), df.columns(28), df.columns(29), df.columns(36)).show(100)

    val haversine = udf((dmsLat:String, dmsLon:String, dmsLat2:String, dmsLon2:String) =>
    {
      val lat1 = (dmsLat.split(":")(0).toDouble / 1 ) + ( dmsLat.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat.split("/")(1).toDouble / 3600 / 1000)
      val lon1 = (dmsLon.split(":")(0).toDouble / 1 ) + ( dmsLon.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon.split("/")(1).toDouble / 3600 / 1000)

      val lat2 = (dmsLat2.split(":")(0).toDouble / 1 ) + ( dmsLat2.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat2.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat2.split("/")(1).toDouble / 3600 / 1000)
      val lon2 = (dmsLon2.split(":")(0).toDouble / 1 ) + ( dmsLon2.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon2.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon2.split("/")(1).toDouble / 3600 / 1000)
      val dLat=(lat2 - lat1).toRadians

      val dLon=(lon2 - lon1).toRadians

      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      val result = R * c
      if(result < 200 && result > 0) 1
      else
        0
    }
    )

    val productDf = defaultDf.alias("df").crossJoin(sideDf.alias("df2")).select(
      col("df._c2"), col("df._c4"), col("df._c5"), col("df._c6"), col("df._c8"), col("df._c13"), col("df._c36"), col("df._c28"), col("df._c29"), col("df2.lat2"), col("df2.lon2"))
      .withColumn("_c99", haversine(col("df._c28"), col("df._c29"), col("df2.lat2"), col("df2.lon2")))


    val addFlag = udf((sumVal:Int) => {
      if(sumVal > 0) "Y"
      else "N"
    })

    val aggrDf = productDf.groupBy("_c2", "_c4", "_c5", "_c6", "_c8", "_c13", "_c36").agg(sum("_c99").alias("_c99")).withColumn("_flag", addFlag(col("_c99"))).drop("_c99")

    val joinDf = defaultDf.alias("df").join(aggrDf.alias("df3"), Seq("_c2", "_c4", "_c5", "_c6", "_c8", "_c13", "_c36"), "left_outer")
    joinDf.write.format("com.databricks.spark.csv").save("/home/hadoop/ykoh/result")

    /*
    val joinDf = defaultDf.alias("df").join(productDf.alias("df3"), col("df._c2") === col("df3._c2") && col("df._c4") === col("df3._c4") && col("df._c5") === col("df3._c5") && col("df._c6") === col("df3._c6") && col("df._c8") === col("df3._c8") && col("df._c13") === col("df3._c13") && col("df._c36") === col("df3._c36"), "left_outer")
      .select(col("df.*"), col("df3._c99"))

    val addFlag = udf((sumVal:Int) => {
      if(sumVal > 0) "Y"
      else "N"
    })
    val aggrDf = joinDf.alias("joinDf")
      .groupBy("_c2", "_c4", "_c5", "_c6", "_c8", "_c13", "_c36").agg(sum("_c99").alias("_c99")).withColumn("_flag", addFlag(col("_c99"))).drop("_c99")
    //        println("aggrDf count : " + aggrDf.count())

    aggrDf.write.format("com.databricks.spark.csv").save("/home/hadoop/ykoh/result")
*/

  }
}
