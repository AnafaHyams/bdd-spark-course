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


    val totalKilometersTripsToBoston = rdd.map(line => line.split(" "))
      .map(x => Trip(Integer.parseInt(x(0)), x(1).toUpperCase, Integer.parseInt(x(2)), LocalDate.now()))
      .filter(trip => trip.city == "BOSTON")
      .map(_.distance)
      .sum()

    println(s"Total kilometers of trips to Boston: $totalKilometersTripsToBoston")

    val drivers_km: RDD[(Int, Int)] = rdd.map(line => line.split(" "))
      .map(x => Trip(Integer.parseInt(x(0)), x(1).toUpperCase, Integer.parseInt(x(2)), LocalDate.now()))
      .groupBy(t => t.driverID)
      .mapValues(list => list.map(trip => trip.distance).sum)
      .sortBy(_._2, ascending = false)
      //.take(3).

    val driversRdd = sc.textFile("data/taxi/drivers.txt")

    val value4: RDD[(Int, String)] = driversRdd.map(line => line.split(", "))
      .map(x => Driver(Integer.parseInt(x(0)), x(1), x(2), x(3)))
      .map(driver => (driver.driverID, driver.name))


    println("Name of 3 drivers with max total kilometers:")
    drivers_km.join(value4).take(3)
      .foreach(tuple => println(tuple._2._2))

  }

}
