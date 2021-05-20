package taxi_lab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

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

  }

}
