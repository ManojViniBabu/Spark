package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  //Creating  a spark session
  val spark = SparkSession.builder()
    .appName("DataFrame Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading  a  DF
  val firstDF = spark.read
    .format(source = "json")
    .option("inferSchema", "true")
    .load(path = "src/main/resources/data/cars.json")
  //Showing a DataFrame
  firstDF.show()
  firstDF.printSchema()

  //distributed collection of  rows confirming  to a schema  -DF

  //Get first  10  rows
  firstDF.take(10).foreach(println)

  /**
    * A Spark schema structure that describes a small cars DataFrame.
    */
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

  //read a data frame  with  our schema
  val carsDFWithSchema = spark.read
    .format(source = "json")
    .schema(carsSchema)
    .load(path = "src/main/resources/data/cars.json")

  carsDFWithSchema.show()
  //create rows  by hand
  //val  myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  //Create  DF from Tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualcarDF = spark.createDataFrame(cars) //schema auto-inferred at runtime
  //schema is applicable to dataframes and not to rows

  //create DF with  implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name","MPG","Cylinders","Displacement","HP","Weight","Acceleration","Year","Country")
  manualcarDF.printSchema()
  manualCarsDFWithImplicits.printSchema()




}
