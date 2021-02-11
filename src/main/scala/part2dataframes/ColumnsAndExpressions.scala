package part2dataframes

import org.apache.spark.sql.functions.{col,column,expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
Operate on DataFrame columns
Obtain  new dataframes with  expression
 */


object ColumnsAndExpressions extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName(name="ColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json(path="src/main/resources/data/cars.json")

  //carsDF.show()

  //Columns are special object whicjh helps to obtain new dataframes from source  dataframe

  val firstColumn = carsDF.col("Name")

  //selecting -- (technical  term  - projections)
  val carNameDF = carsDF.select(firstColumn)

  //carNameDF.show()
  import spark.implicits._
  //various select methods
   carsDF.select(
     carsDF.col("Name"),
     col("Acceleration"),
     column("Weight_in_lbs"),
     'Year, //Scala Symbol,auto converted to column
     $"Horsepower", // fancier  interpolated  string ,returns a column object
     expr("Origin") //expression
   )

  //Select  plain columns
   carsDF.select("Name",  "Year")

  //narrow transformation
  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs")/2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weights_in_kgs"),
    expr("Weight_in_lbs/2.2").as("Weight_in_kg_2")

  )
  //carsWithWeightsDF.show()

  val carsWithSelectExprDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2  as Weight"

  )
  //carsWithSelectExprDF.show()

  //DF Processing

  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_Kg_3",col("Weight_in_lbs")/2.2)
  //Renaming a column
  val carsWithRenamedColumnDF = carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")
  //careful with column names
  carsWithRenamedColumnDF.selectExpr("`Weight in pounds`")
  carsWithRenamedColumnDF.drop("Cylinders","Displacement")

  //filtering
  val  europeancarsDF = carsDF.filter(col("Origin")==="Europe")
  val  notEuropeancarsDF = carsDF.where(col("Origin")=!="Europe")
  //notEuropeancarsDF.show()
  //without col
  val  usCarsDF = carsDF.filter("Origin='USA'" )
 // usCarsDF.show()

  //chain filters
  val  usPowerfulCarsDF  = carsDF.filter(col("Origin")==="USA").filter(col("Horsepower") > 150)
  val  usPowerfulCarsDF2 = carsDF.filter((col("Origin")==="USA") and (col("Horsepower") > 150))
  val  usPowerfulCarsDF3 = carsDF.filter((col("Origin")==="USA") .and (col("Horsepower") > 150))
  val  usPowerfulCarsDF4 = carsDF.filter("Origin ='USA' and Horsepower >150")
  usPowerfulCarsDF4.show()
  //unioning
  val moreCarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/more_cars.json")
  val unionCarsDF = carsDF.union(moreCarsDF)
  unionCarsDF.show()

  //distinct values
  val distinctOriginCarsDF = carsDF.select("Origin").distinct()
  distinctOriginCarsDF.show()






}

