import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col

/**
  * Created by Administrator on 2017-05-18.
  */
object CoordinateDistance {
  var R = 6372800 //meter
  def main(args: Array[String]): Unit ={

    val logFile = args(0) //path
    val conf = new SparkConf().setAppName("CoordinateDistance") //conf set
    val sc = new SparkContext(conf)

    val data = sc.textFile(logFile, 2).cache() //rrd instance
    val originalRRD = data.distinct()//.map(_.split("\\|").mkString("|"))//.filter(f=>f.split("\\|")(28)!="" && f.split("\\|")(29)!="")
      //    val originalRRD = data.distinct().map(_.split("\\|")).map(_._).map(f=>(f(1), f(2), f(3), f(4), f(5), f(6), f(7), f(8), f(9), f(28), f(29), f(37)))
//
//    val coorRRD= data.map(_.split("\\|")).map ( c =>
//      ((c(28).split(":")(0).toDouble / 1 ) + ( c(28).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(28).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(28).split("/")(1).toDouble / 3600 / 1000)) + "," + ((c(29).split(":")(0).toDouble / 1 ) + ( c(29).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(29).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(29).split("/")(1).toDouble / 3600 / 1000))
//    )
//

//    originalRRD.take(100).foreach(println)


/*

    //gps x,y
    val coorTake= originalRRD.map { c =>
      val dmsLat = c.split("\\|")(28)
      val dmsLon = c.split("\\|")(29)
      val lat = (dmsLat.split(":")(0).toDouble / 1 ) + ( dmsLat.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat.split("/")(1).toDouble / 3600 / 1000)
      val lon = (dmsLon.split(":")(0).toDouble / 1 ) + ( dmsLon.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon.split("/")(1).toDouble / 3600 / 1000)
      //      ((c(28).split(":")(0).toDouble / 1 ) + ( c(28).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(28).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(28).split("/")(1).toDouble / 3600 / 1000)) + "," + ((c(29).split(":")(0).toDouble / 1 ) + ( c(29).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(29).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(29).split("/")(1).toDouble / 3600 / 1000))
      lat + "|" + lon
    }
*/



    //( c(28).split(":")(0).toDouble / 1 ) + ( c(28).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(28).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(28).split("/")(1).toDouble / 3600 / 1000) + "," + (( c(29).split(":")(0).toDouble / 1 ) + ( c(29).split(":")(1).split("\\.")(0).toDouble / 60 ) + ( c(29).split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( c(29).split("/")(1).toDouble / 3600 / 1000)))
//    val coorTake = coorRRD.take(10000)

//    val coorTakeRRD = sc.makeRDD(coorTake.take(10000))
    val coorTakeRRD = sc.makeRDD(originalRRD.take(10000))

//    coorTakeRRD.take(100).foreach(println) //data print

    //----------------------leftouterjoin test
//    val productRRD = coorRRD.leftOuterJoin(coorTakeRRD)
//    val productRRD2 = productRRD.map(r=>Tuple3(r._1, r._2._1, if(haversine(r.x._1, r._2._1, r._1, r._2._2.getOrElse(0) )<200  /*&& haversine(r._1, r._2._1, r._1, r._2._2.getOrElse(0))>0 */) "true" else "false"))
//    val resultRRD = productRRD2.filter(_._3 == "true")


    val productRRD = originalRRD.cartesian(coorTakeRRD)

/*

    val aggrRRD = productRRD.map(r=>Tuple6(r._1.split(",")(0), r._1.split(",")(1), r._2.split(",")(0), r._2.split(",")(1),
      haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble),
      if( haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble)<200
        && haversine(r._1.split(",")(0).toDouble, r._1.split(",")(1).toDouble, r._2.split(",")(0).toDouble, r._2.split(",")(1).toDouble) > 0 ) "true"
      else
        "false"
    ))
    //aggrRRD.take(1000).foreach(println) //data print
*/
/*

    val aggrRRD = productRRD.map { c=>
      val dmsLat = c._1.split("\\|")(28)
      val dmsLon = c._1.split("\\|")(29)
      val lat = (dmsLat.split(":")(0).toDouble / 1 ) + ( dmsLat.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat.split("/")(1).toDouble / 3600 / 1000)
      val lon = (dmsLon.split(":")(0).toDouble / 1 ) + ( dmsLon.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon.split("/")(1).toDouble / 3600 / 1000)
      (c._1, if( haversine(lat.toDouble, lon.toDouble, c._2.split("\\|")(0).toDouble, c._2.split("\\|")(1).toDouble)<200
        && haversine(lat.toDouble, lon.toDouble, c._2.split("\\|")(0).toDouble, c._2.split("\\|")(1).toDouble) > 0 ) "true"
      else
        "false"
      )
    }//.filter(f=>f._2 == "true")
*/
/*

    val aggrRRD = productRRD.map {
      c=>
        val dmsLat = c._1.split("\\|")(28)
        val dmsLon = c._1.split("\\|")(29)
        val lat = (dmsLat.split(":")(0).toDouble / 1 ) + ( dmsLat.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat.split("/")(1).toDouble / 3600 / 1000)
        val lon = (dmsLon.split(":")(0).toDouble / 1 ) + ( dmsLon.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon.split("/")(1).toDouble / 3600 / 1000)
        (c._1, if( haversine(lat.toDouble, lon.toDouble, c._2.split("\\|")(28).toDouble, c._2.split("\\|")(29).toDouble)<200
          && haversine(lat.toDouble, lon.toDouble, c._2.split("\\|")(28).toDouble, c._2.split("\\|")(29).toDouble) > 0 ) "true"
        else
          "false"
        )
    }.filter(f=>f._2 == "true")
//    aggrRRD.take(100).foreach(println)
*/
    val aggrRRD = productRRD.map {
      c=>
        val dmsLat = c._1.split("\\|")(28)
        val dmsLon = c._1.split("\\|")(29)
        val lat = (dmsLat.split(":")(0).toDouble / 1 ) + ( dmsLat.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat.split("/")(1).toDouble / 3600 / 1000)
        val lon = (dmsLon.split(":")(0).toDouble / 1 ) + ( dmsLon.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon.split("/")(1).toDouble / 3600 / 1000)
        val p_cuid = c._1.split("\\|")(8)
        val sisul_code = c._1.split("\\|")(2)
        val sector = c._1.split("\\|")(4)
        val sequence_id = c._1.split("\\|")(6)
        val branch_id = c._1.split("\\|")(13)
        val rmod_id = c._1.split("\\|")(36)

        val dmsLat2 = c._2.split("\\|")(28)
        val dmsLon2 = c._2.split("\\|")(29)
        val lat2 = (dmsLat2.split(":")(0).toDouble / 1 ) + ( dmsLat2.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLat2.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLat2.split("/")(1).toDouble / 3600 / 1000)
        val lon2 = (dmsLon2.split(":")(0).toDouble / 1 ) + ( dmsLon2.split(":")(1).split("\\.")(0).toDouble / 60 ) + ( dmsLon2.split("\\.")(1).split("/")(0).toDouble / 3600 ) + ( dmsLon2.split("/")(1).toDouble / 3600 / 1000)
        ((p_cuid, sisul_code, sector, sequence_id, branch_id, rmod_id),
          if( haversine(lat.toDouble, lon.toDouble, lat2.toDouble, lon2.toDouble)<200
          && haversine(lat.toDouble, lon.toDouble, lat2.toDouble, lon2.toDouble) > 0 ) 1
        else
          0)
    }
    .filter(f=>f._2 == 1).keyBy(f=>f._1)

    val grpRRD = originalRRD.map{c=>
      val k = c.split("\\|")
      val p_cuid = k(8)
      val sisul_code = k(2)
      val sector = k(4)
      val sequence_id = k(6)
      val branch_id = k(13)
      val rmod_id = k(36)
      ((p_cuid, sisul_code, sector, sequence_id, branch_id, rmod_id),
      c)
    }.keyBy(c=>c._1)

    val joinRRD = grpRRD.leftOuterJoin(aggrRRD,60)
    val resultRRD = joinRRD.map(c=>
      {(c._1,
        c._2._2 match {
          case Some(c1) => c1._2
          case None => 0
        }
      )}
    )
    .reduceByKey(_+_).map(c=>(c._1, if(c._2 > 0) "Y" else "N"))
    resultRRD.take(100).foreach(println)
/*

    //groupbyKey
    val grpRRD = aggrRRD.repartition(60).map{x=>
          var column = x._1.split("\\|")
          val p_cuid = column(3)
          val sisul_code = column(5)
          val sector = column(6)
          val sequence_id = column(7)
          val branch_id = column(9)
          val rmod_id = column(37)
          ((p_cuid, sisul_code, sector, sequence_id, branch_id, rmod_id), x)

        }.groupByKey(60).mapValues(_.maxBy(_._2))
*/
/*
    groupBy{k =>
      val column = k._1.split("\\|")
      val p_cuid = column(3)
      val sisul_code = column(5)
      val sector = column(6)
      val sequence_id = column(7)
      val branch_id = column(9)
      val rmod_id = column(37)
      (p_cuid, sisul_code, sector, sequence_id, branch_id, rmod_id)}.mapValues(_.maxBy(_._2))
    */
//    aggrRRD.saveAsTextFile("file:///home/hadoop/ykoh/result")

/*  //test
    val rrd1 = sc.parallelize(List((1,2), (3,4), (5,6), (1,2)), 1)
    val rrd2 = sc.parallelize(List((1,3), (2,5), (3,3)), 1)

    val joinRRD = rrd1.cartesian(rrd2)
//    joinRRD.collect().foreach(println)

//    val resultRRD = joinRRD.map(r=>Tuple6(r._1.x._1, r._1.x._2, r._2._1, r._2._2, haversine(r._1._1, r._1._2, r._2._1, r._2._2),
    val resultRRD = joinRRD.map(r=> (r._1,r._2,
      if(haversine(r._1._1, r._1._2, r._2._1, r._2._2) > 13)
        1
      else
        0
    )).filter(f=>f._3==1).keyBy(c=>c._1)

    resultRRD.collect().foreach(println)

  val rrdleft = rrd1.map(c=>(c._1, c._2)).keyBy(c=>c.x)
    rrdleft.foreach(println)

    val reRRD = rrdleft.leftOuterJoin(resultRRD, 1)
      .map(c=>{ (c._1,
      c._2._2 match {
        case Some(c1) => c1._3
        case None => 0
      })
    })
    .reduceByKey(_+_).map(c=>(c._1, if(c._2>0) "Y" else "N"))

    val reRRD = resultRRD.leftOuterJoin(rrdleft, 1).map(c=>{(c._1,
      c._2._1._3
    )

    })
    val reRRD = rrdleft.join(resultRRD, 1)
    reRRD.collect().foreach(println)
//    resultRRD.groupBy(k=>(k._1,k._2)).mapValues(_.maxBy(_._6)).map(f=>(f._1._1, f._1._2, f.x._2._6)).collect().foreach(println)
//    resultRRD.groupBy(k=>(k._1,k._2)).collect().foreach(println)
    //rrdleft.collect().foreach(println)

    sc.stop()
*/
/*
    val df4 = df3.withColumn("_c99", haver(col("df._c28"), col("df._c29"), col("df2._c28"), col("df2._c29")))
//    df4.show(100);
    val df5 = df4.groupBy(col("df._c2"), col("df._c4"),
      col("df._c6"), col("_c8"), col("_c13"), col("_c36")).agg(sum("_c99"))
    println(df5.count())*/
//    df5.show(100)
  }

  import scala.math._
  def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double={
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }
  def haversine(lat1:Int, lon1:Int, lat2:Int, lon2:Int):Int={
    lat1 + lon1 + lat2 + lon2
  }
}
