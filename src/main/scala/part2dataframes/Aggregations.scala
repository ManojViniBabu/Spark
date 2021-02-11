package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val  spark  = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show(1)
  //counting
  val genreCountDF = moviesDF.select(count("Major_Genre"))//all values except null
  genreCountDF.show()
  //all  count
  val allColumnCountDF = moviesDF.select(count("*"))
  allColumnCountDF.show()// all values including nulls
 //Distinct Genre
  moviesDF.select(countDistinct("Major_Genre").as("distinct")).show()

  //for quick data analysis
  moviesDF.select(approx_count_distinct("Major_Genre")).show()

  //min and max
  val minIMDBRatingDF = moviesDF.select(min("IMDB_Rating"))
  val maxIMDBRatingDF = moviesDF.select(max("IMDB_Rating"))
  minIMDBRatingDF.show()
  maxIMDBRatingDF.show()
  //selectExpr
  moviesDF.selectExpr("min(Major_Genre)").show()

  //sum and  average

  moviesDF.select (avg("Worldwide_Gross")).show()

  moviesDF.select (sum("Worldwide_Gross")).show()

  //mean  and standard deviation

  moviesDF.select (
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating")
  ).show()

  //Grouping

  val countByGenreDF = moviesDF.groupBy("Major_Genre") //GROUP BY INCLUDES null
    .count() // select Major_Genre ,count(*) from  movies  group by  Major_Genre

  countByGenreDF.show()

  val avgIMDBByGenreDF = moviesDF.groupBy("Major_Genre") //GROUP BY INCLUDES null
    .avg("IMDB_Rating")

  avgIMDBByGenreDF.show()

  val  aggByGenreDF = moviesDF.groupBy("Major_Genre") //GROUP BY INCLUDES null
    .agg(
      count("*").as("N_Count"),
      avg("IMDB_Rating").as("Avg_Rating")
  )
    .orderBy("Avg_Rating")

  aggByGenreDF.show()


}