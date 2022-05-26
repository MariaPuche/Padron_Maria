package Cap06.Spark_SQL_and_Datasets.Transformig_sample_data

import org.apache.spark.sql.{Encoders, SparkSession}

object Usagees extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Cap06.WWD")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  //our cas class for the dataset
  val r = new scala.util.Random(42)

  // Create 1000 instances of scala Usage class
  // This generates data on the fly

  val data = for (i <- 0 to 1000) yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))
  //is needed to encode after create the dataset
  val usageDS = spark.createDataset(data)
  //usageDS.show()
  //using high functions and functional programming
  // In Scala
  // Use an if-then-else lambda expression and compute a value
  usageDS.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
    .show(5, false)

  def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * 0.15 else usage * 0.50
  }
  // Use the function as an argument to map()
  usageDS.map(u => {computeCostUsage(u.usage)}).show(5, false)

  // Compute the usage cost with Usage as a parameter
  // Return a new object, UsageCost
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
    UsageCost(u.uid, u.uname, u.usage, v)
  }
  // Use map() on our original Dataset
  usageDS.map(u => {computeUserCostUsage(u)}).show(5)

  }