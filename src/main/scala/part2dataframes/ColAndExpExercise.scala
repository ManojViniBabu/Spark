package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,column,expr}

object ColAndExpExercise extends  App {

  /*
  -->Read the movies  dataframe  and read any 2 columns  of your choice
  -->Create another column summing up the profit  of the movie US_Gross + WorldWide_Gross + DVDSales
  -->Select  all  comedy  movies with  IMDB rating above 6

--> Use  as  many ways  as possible

   */

  val  spark = SparkSession.builder()
    .appName("ColAndExpExercise")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //moviesDF.show()


  //Way 1
  val anyTwoColumnDF1 = moviesDF.select(col("Title"),col("US_Gross"))
  anyTwoColumnDF1.show()

  //Way 2
  val firstColumn = moviesDF.col("Director")
  val secondColumn = moviesDF.col("Title")

  val anyTwoColumnDF2 = moviesDF.select(firstColumn,secondColumn)
  anyTwoColumnDF2.show()

  //Way 3
  import spark.implicits._
  val anyTwoColumnDF3 = moviesDF.select(
    'US_DVD_Sales,
    'Title

  )
 anyTwoColumnDF3.show()
 //Way4
  val anyTwoColumnDF4 = moviesDF.select(
    $"Title",
    $"Worldwide_Gross"
  )
  anyTwoColumnDF4.show()

  //way5
  val anyTwoColumnDF5 = moviesDF.select(
    moviesDF.col("Release_Date"),
    moviesDF.col("Production_Budget")
  )
  anyTwoColumnDF5.show()
  //way6

  val anyTwoColumnDF6 = moviesDF.select(
    column("Major_Genre"),
    column("Creative_Type")
  )
  anyTwoColumnDF6.show()

  //way7

  val anyTwoColumnDF7 = moviesDF.select(
    expr("Rotten_Tomatoes_Rating"),
    expr("IMDB_Votes")
  )
  anyTwoColumnDF7.show()

  //Create another column summing up the profit  of the movie US_Gross + Worldwide_Gross + US_DVD_Sales
 //Way 1
  val sumProfitDF = moviesDF.select(
    expr("US_Gross + Worldwide_Gross + US_DVD_Sales as Net_Profit")
  )
  sumProfitDF.show()
  //Way 2
  val sumProfitDF2 = moviesDF.selectExpr(
    "US_Gross + Worldwide_Gross  as Net_Profit"
  )
  sumProfitDF2.show()

  //way3
  val sumProfitDF3 = moviesDF.withColumn("Net_Profit",col("US_Gross")+col("Worldwide_Gross"))
  val filteredSumProfitDF3 = sumProfitDF3.where(col("Net_Profit")>0)
  filteredSumProfitDF3.show()



  //Select  all  comedy  movies with  IMDB rating above 6
  val goodComedyDF = moviesDF.select("Title").filter((col("Major_Genre")==="Comedy").and(col("IMDB_Rating")>6))
  goodComedyDF.show()

  val goodComedyDF2 = moviesDF.select("Title").where((col("Major_Genre")==="Comedy").and(col("IMDB_Rating")>6))
  goodComedyDF2.show()
  val goodComedyDF3 = moviesDF.select("Title","IMDB_Rating")
    .where(col("Major_Genre")==="Comedy")
    .where(col("IMDB_Rating")>6)
  goodComedyDF3.show
}
