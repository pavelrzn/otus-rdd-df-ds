package homework2

import homework2.UDFLib.processRmsd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, date_format, _}

import java.text.SimpleDateFormat
import java.util.Properties

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"


  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZones = spark.read.option("header", value = true).csv("src/main/resources/data/taxi_zones.csv")

  taxiFactsDF.printSchema()
  taxiFactsDF.show(5)

  // task 1
  println("\n\n\t>>>>>>>> TASK 1 >>>>>>>>")

  val locations = taxiFactsDF
    .select(col("DOLocationID").alias("LocationID"))
    .join(taxiZones, Seq("LocationID"), "left")
    .groupBy("Borough").count()
    .sort(col("count").desc)

  locations.show(false)

  locations.coalesce(1)
    .write.format("parquet")
    .mode("overwrite")
    .save("popLocations.parquet")



  // task 2
  println("\n\n\t>>>>>>>> TASK 2 >>>>>>>>")

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH")
  val taxiFactsRdd = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018").rdd

  val popTimes: RDD[String] = taxiFactsRdd
    .map(row => dateFormat.format(row.getTimestamp(1)) -> 1)
    .reduceByKey((a, b) => a + b)
    .sortBy(_._2)
    .map(tup => tup._1 + " " + tup._2)
    .cache()

  popTimes.foreach(println(_))
  popTimes.saveAsTextFile("popTimes")



  // task 3
  println("\n\n\t>>>>>>>> TASK 3 >>>>>>>>")

  val connectionProperties = new Properties()

  connectionProperties.put("user", user)
  connectionProperties.put("password", password)

  val tripDistance = "trip_distance"
  val statsDf = taxiFactsDF.select("tpep_pickup_datetime", tripDistance)
    .withColumn("date", date_format(col("tpep_pickup_datetime"), "dd-MM-yyyy"))
    .drop("tpep_pickup_datetime")
    .groupBy("date")
    .agg(count(tripDistance).as("count"),
      min(tripDistance).as("min"),
      max(tripDistance).as("max"),
      avg(tripDistance).as("avg"),
      collect_list(tripDistance).as("list"))
    .withColumn("RMSD",
      processRmsd(col("list"), col("avg"))) // среднеквадратичное отклонение
    .drop("list")
    .persist()

  statsDf.show(true)

  statsDf.write
    .mode("overwrite")
    .jdbc(url = url, table = "distance_statistics", connectionProperties = connectionProperties)

}

