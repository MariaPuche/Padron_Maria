package Cap02.Starting

import org.apache.spark.sql.SparkSession

object Main extends App {

  import org.apache.spark.sql.functions._

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL basic example")
    .getOrCreate()

  val mnmDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("mnm_dataset.csv")

  val countMnMDF = mnmDF
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy(desc("Total"))

  countMnMDF.show(60)
  println(s"Total Rows = ${countMnMDF.count()}")
  println()
}
