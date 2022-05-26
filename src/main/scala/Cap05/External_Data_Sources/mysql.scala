package Cap05.External_Data_Sources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, rank, sum}

object mysql extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("mysql")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val departmentsDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("dbtable", "departments")
    .option("user", "root")
    .option("password", "m4mg8hJB")
    .load()

  val employeesDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("dbtable", "employees")
    .option("user", "root")
    .option("password", "m4mg8hJB")
    .load()

  val salaryDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("dbtable", "salaries")
    .option("user", "root")
    .option("password", "m4mg8hJB")
    .load()


  val titleDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("dbtable", "titles")
    .option("user", "root")
    .option("password", "m4mg8hJB")
    .load()

  val dept_empDF = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees")
    .option("dbtable", "dept_emp")
    .option("user", "root")
    .option("password", "m4mg8hJB")
    .load()

  //Utilizando operaciones de ventana obtener el salario, posición (cargo)
  //y departamento actual de cada empleado, es decir, el último o más
  //reciente

  val window1 = Window.partitionBy("emp_no").orderBy("con.to_date")

  val consulta = employeesDF.alias("emp")
    .join(salaryDF.as("sal"), "emp_no")
    .join(titleDF, "emp_no")
    .join(dept_empDF.as("con"), "emp_no")
    .join(departmentsDF, "dept_no")
    .withColumn("rank", rank().over(window1))
    .withColumn("maxrank", max("rank").over(window1))
    .withColumn("sumSalary", sum("salary").over(window1))
    .select("first_name", "sumSalary", "dept_name", "con.from_date", "con.to_date", "rank", "maxrank", "title").distinct()
    .where("rank == maxrank")
    .show(15)

}
