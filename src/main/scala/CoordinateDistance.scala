import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-05-18.
  */
object CoordinateDistance {
var R = 6372800 //meter
  def main(args: Array[String]): Unit = {
    val logFile = args(0) //path
    val conf = new SparkConf().setAppName("CoordinateDistance") //conf set
    val sc = new SparkContext(conf)

    val data = sc.textFile(logFile, 2).cache() //rrd instance
    val coorRRD= data.map(_.split("\\|")).map ( c =>
      ((c(28).split(":")(0).toDouble / 1 ) + ( c(28).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(28).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(28).split("/")(1).toDouble / 3600 / 1000)) + "," + ((c(29).split(":")(0).toDouble / 1 ) + ( c(29).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(29).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(29).split("/")(1).toDouble / 3600 / 1000))
    )

    //( c(28).split(":")(0).toDouble / 1 ) + ( c(28).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(28).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(28).split("/")(1).toDouble / 3600 / 1000) + "," + (( c(29).split(":")(0).toDouble / 1 ) + ( c(29).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(29).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(29).split("/")(1).toDouble / 3600 / 1000)))

    val coorTake = coorRRD.take(3)
    val coorTakeRRD = sc.makeRDD(coorTake)

//    coorTakeRRD.collect().foreach(println) //data print

    //----------------------leftouterjoin test
//    val productRRD = coorRRD.leftOuterJoin(coorTakeRRD)
//    val productRRD2 = productRRD.map(r=>Tuple3(r._1, r._2._1, if(haversine(r.x._1, r._2._1, r._1, r._2._2.getOrElse(0) )<200  /*&& haversine(r._1, r._2._1, r._1, r._2._2.getOrElse(0))>0 */) "true" else "false"))
//    val resultRRD = productRRD2.filter(_._3 == "true")


    val productRRD = coorRRD.cartesian(coorTakeRRD)

//    resultRRD.collect().foreach(println) //data print
//    println(s"@@@Count coorRRD : " +  coorRRD.count() + ", coorTakeRRD : " + coorTakeRRD.count() + " productRRD : " + productRRD.count() + ", resultRRD : " + resultRRD.count() + ", productRRD2 : " + productRRD2.count())
//    println(s"@@@Count coorRRD : " +  coorRRD.count() + ", coorTakeRRD : " + coorTakeRRD.count() + " productRRD : " + productRRD.count())


    val aggrRRD = productRRD.map(r=>Tuple6(r._1.split(",")(0), r._1.split(",")(1), r._2.split(",")(0), r._2.split(",")(1),
      haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble),
      if( haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble)<200
        && haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble) > 0 ) "true"
      else
        "false"
    ))
    //aggrRRD.take(1000).foreach(println) //data print


    val grpRRD = aggrRRD.groupBy(k => (k._1, k._2)).values

    grpRRD.collect().foreach(println)


//    aggrRRD.saveAsTextFile("file:///home/hadoop/ykoh/result")

/*

    val rrd1 = sc.parallelize(List((1,2), (3,4), (5,6), (1,2)), 1)
    val rrd2 = sc.parallelize(List((1,3), (2,5), (3,3)), 1)


    val joinRRD = rrd1.cartesian(rrd2)
    joinRRD.collect().foreach(println)

    val resultRRD = joinRRD.map(r=>Tuple6(r._1.x._1, r._1.x._2, r._2._1, r._2._2, haversine(r._1._1, r._1._2, r._2._1, r._2._2),
      if(haversine(r._1._1, r._1._2, r._2._1, r._2._2) > 13)
        "Y"
      else
        "N"
    ))
*/



//    resultRRD.groupBy(k=>(k._1,k._2)).mapValues(_.maxBy(_._6)).map(f=>(f._1._1, f._1._2, f.x._2._6)).collect().foreach(println)
//    resultRRD.groupBy(k=>(k._1,k._2)).collect().foreach(println)
//    resultRRD.collect().foreach(println)

    sc.stop()
  }

  import scala.math._
  def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }
  def haversine(lat1:Int, lon1:Int, lat2:Int, lon2:Int)={
    lat1 + lon1 + lat2 + lon2
  }
}
