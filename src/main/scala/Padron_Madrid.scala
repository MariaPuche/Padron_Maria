import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Padron_Madrid extends App {

  // Crear la sesión de Spark
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Padron Madrid")
    .getOrCreate()

  import spark.implicits._

  // Cargar los datos sin espacios necesarios, sustituyendo los valores vacíos por 0 y con esquema.
  val padron = spark.read.format("csv")
    .option("header","true")
    .option("inferschema","true")
    .option("emptyValue",0)
    .option("delimiter",";")
    .load("Rango_Edades_Seccion_202205.csv")
    .withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
    .withColumn("DESC_BARRIO",trim(col("desc_barrio")))

  padron.show()

  //  Enumerar todos los barrios diferentes. (131 barrios diferentes)
  padron.select(countDistinct("desc_barrio").alias("Numero_barrios")).show()

  // Crea una vista temporal y, a través de ella, cuenta el número de barrios diferentes que hay.
  padron.createOrReplaceTempView("padron")

  spark.sql("""SELECT count(distinct(desc_barrio)) FROM padron""").show()

  // Crea una nueva columna "longitud" con la longitud de los campos de la columna DESC_DISTRITO.
  val padron3 = padron.withColumn("longitud", length(col("desc_distrito")))
  padron3.show(5, false)

  // Crear otra columna que muestre el valor 5 para cada fila.
  // Lit(): Para añadir nuevas columnas a un dataframe con valor constate.
  val padron4 = padron3.withColumn("Valor5",lit(5))
  padron4.show(5,false)

  // Borrar la columna valor5
  val padron5 = padron4.drop("Valor5")
  padron5.show(5, false)

  // Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO
  val padron_particionado = padron5.repartition(col("Desc_Distrito"),col("Desc_barrio"))

  // Almacena la partición en el cache
  padron_particionado.cache()

  // Muestre el total de espanoleshombres, espanolesmujeres, extranjeroshombres, extranjerosmujeres para cada barrio de cada distrito.
  // Las columnas distrito y barrio deben ser las primeras en aparecer, los resultados se deben mostrar ordenados según
  // "extranjerosmujeres" y desempatará por "extranjeroshombres"
  padron_particionado
    .groupBy("desc_barrio","desc_distrito")
    .agg(count("espanolesHombres").alias("espanolesHombres"),
         count("espanolesMujeres").alias("espanolesMujeres"),
         count("extranjerosHombres").alias("extranjerosHombres"),
         count("extranjerosMujeres").alias("extranjerosMujeres"))
    .orderBy(desc("extranjerosMujeres"),desc("extranjerosHombres"))
    .show(5, false)

  // Eliminar todo el cache de las tablas de la memoria cache
  spark.catalog.clearCache()

  // Crear un nuevo DataFrame a partir del original que muestre solo la columna Desc_Barrio, Desc_distrito y
  // número total de "espanoleshombres" de cada distrito y barrio.
  val padron_new = padron. groupBy("desc_barrio","desc_distrito")
    .agg(count("espanoleshombres").alias("Total de hombres Españoles"))

  padron_new.show(5, false)

  // Crea un nuevo DataFrame que muestre todas las columnas de padron mas una nueva con el total de hombres españoles
  import org.apache.spark.sql.expressions.Window

  val padronWindow = padron
    .withColumn("TotalEspHom",
      sum(col("espanoleshombres"))
        .over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))

  padronWindow.show(5, false)

  // Mediante una función Pivot muestra una tabla que contenga los valores totales de espanolesmujeres para cada distrito y
  // en cada rango de edad . Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas.
    val padron_pivot = padron_particionado
      .groupBy("cod_edad_int")
      .pivot("desc_distrito", Seq("BARAJAS","CENTRO","RETIRO") )
      .sum("espanolesMujeres")
      .orderBy(col("cod_edad_int"))

  padron_pivot.show(5, false)

  // Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje
  //de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa
  //cada uno de los tres distritos. Debe estar redondeada a 2 decimales.
  val padron_porcen = padron_pivot
    .withColumn("porcentaje_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
    .withColumn("porcentaje_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
    .withColumn("porcentaje_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

  padron_porcen.show(5, false)

  // Guarar el archivo csv original particionado por distrito y por barrio en un directorio local.
  padron.write.format("csv")
    .option("header","true")
    .mode("overwrite")
    .partitionBy("desc_distrito","desc_barrio")
    .save("C:/Users/maria.puche/Desktop/Padron_Madrid/Spark/Spark_csv")

  // Guardar el archivo original particionado por distrito y por barrio en un directorio local en formato parquet
  padron.write.format("parquet")
    .option("header","true")
    .mode("overwrite")
    .partitionBy("desc_distrito","desc_barrio")
    .save("C:/Users/maria.puche/Desktop/Padron_Madrid/Spark/Spark_parquet")
}
