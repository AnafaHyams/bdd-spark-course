package taxi_lab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object MainTaxiLabScala {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")

    val sparkConf = new SparkConf().setAppName("Taxi spark from scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    val rdd = sc.textFile("data/taxi/trips.txt")

    val lines = rdd.count()
    println(s"Number of lines: $lines")

    val value: RDD[Array[String]] = rdd.map(line => line.split(" "))
    val value1: RDD[Trip] = value.map(x => Trip(Integer.parseInt(x(0)), x(1).toUpperCase, Integer.parseInt(x(2)), LocalDate.now()))
    val value2: RDD[Trip] = value1.filter(trip => trip.city == "BOSTON" && trip.distance > 10)
    val bostonTripMoreThan10 = value2.count()
    println(s"Number of trips to Boston longer than 10KM: $bostonTripMoreThan10")

//    value.map(l => new Trip(l.))
//    value.map(line => Tuple4(_1 = line[0],_2 = line[1],_3 = line[2],_4 = line[3]))
  }

}
