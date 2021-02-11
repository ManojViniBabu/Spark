package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, _}


object DataSources extends App {

  //Creating  a spark session
  val spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // reading  a  DF
  //Needs the  following
  /*
   -format
   -schema or infer schema='true'(optional)
   -zero or more options

   */

  val carsDF = spark.read
    .format(source = "json")
    .schema(carsSchema) //enforce a schema
    .option("mode","failFast")   //mode decides what spark should do  in case of  malformed  record
    //other options -dropMalformed  rows , permissive (default)
    .option("path","src/main/resources/data/cars.json")
    .load()

  //carsDF.show()
  //alternative to load

  /*
  val carsDF = spark.read
    .format(source = "json")
    .schema(carsSchema) //enforce a schema
    .option("mode","failFast")   //mode decides what spark should do  in case of  malformed  record
    //other options -dropMalformed  rows , permissive (default)
    .load(path = "src/main/resources/data/cars.json")

   */

  val  carsWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" ->  "src/main/resources/data/cars.json",
      "inferSchema" -> "true"

    )
    )
    .load()

  //carsWithOptionMap.show()
  /*
  Writing DFs
  -format
  -save mode =overwrite ,append ,ignore ,errorIfExists
  -path
  -zero or more  options
   */
/*
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save(path="src/main/resources/data/cars_dupe.json")
*/
  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat","yyyy-MM-dd")//couple with schema  if spark fails to parse else it will go as null
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed")//bzip2,gzip,lz4,snappy,deflate
    .json(path="src/main/resources/data/cars.json")// we  don't need to use format and load()
  //  .show()

  //CSV Flags
    val stockSchema = StructType(Array(
      StructField("symbol",StringType),
      StructField("date",DateType),
      StructField("price",DoubleType)
    ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat","MMM d yyyy")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .csv(path="src/main/resources/data/stocks.csv")
    .show()

  ///Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save(path="src/main/resources/data/cars.parquet")

  //text files
  //spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading  from  a DB
  val  employeesDF = spark.read
    .format(source="jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employeesDF.show()



}