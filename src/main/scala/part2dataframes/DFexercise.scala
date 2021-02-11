package part2dataframes

import org.apache.spark.sql.SparkSession
import part2dataframes.DataFramesBasics.{cars, spark}

object DFexercise extends App {
  //exercise
  /*1) Create  a  manual  DF describing  smartphones
      -make
      -model
      -screen_dimension
      -camera megapixels

    2)Read another folder from  data folder eg movies.json
    -print its schema
     - count  the number of rows


   */

  //Creating  a spark session
  val  spark = SparkSession.builder()
    .appName(name="DF Exercise")
    .config("spark.master","local")
    .getOrCreate()

  val smartphones = Seq(
    ("Samsung","S10","6inch","1920x1080px"),
    ("Apple","IphoneX","5inch","2080x1080px"),
    ("Nokia","1100","2inch","20x10px"),
    ("mi","A3","4inch","1360x568")
  )
  import spark.implicits._
  val manualSmartphoneDFWithImplicits = smartphones.toDF("Make","Model","Screen_Dimension","Camera_Quality")
  manualSmartphoneDFWithImplicits.show()
  manualSmartphoneDFWithImplicits.printSchema()


  //2
  val moviesDF = spark.read
    .format(source = "json")
    .option("inferSchema", "true")
    .load(path = "src/main/resources/data/movies.json")
  //Showing a DataFrame
  moviesDF.show()
  moviesDF.printSchema()
  println(moviesDF.count())
}
