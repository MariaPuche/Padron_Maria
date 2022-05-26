package Cap06.Spark_SQL_and_Datasets.Scala_Case

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object Bloggers extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Bloggers")
    .getOrCreate()

  val BloggerEncoder = Encoders.bean(classOf[Blog])

  val path: String = "blogs.json"
  val bloggersDS: Dataset[Blog] = spark.read.format("json").option("path", path).load.as(BloggerEncoder)

  bloggersDS.show()
}
