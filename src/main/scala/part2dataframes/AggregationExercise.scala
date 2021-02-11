package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationExercise extends App  {
  /*
  -->Sum up all the profits in DF
  -->count  how many  distinct  directors we have
  -->Show the  mean  and  standard deviation of US  gross revenue for the movies
  -->Compute  the average IMDB  rating  and  the average  US gross revenue PER  DIRECTOR
   */

  val spark  = SparkSession.builder()
    .appName("Aggregations Exercise")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show(1)

 //way 1
  val sumProfitsDF = moviesDF.agg(
    ( sum("US_Gross" )+ sum("Worldwide_Gross") + sum("US_DVD_Sales") ).as("Net_sum_profit")

  )
  sumProfitsDF.show()


 // way 2
  //selectExpr
  moviesDF.selectExpr("sum(US_Gross) + sum(Worldwide_Gross) + sum(US_DVD_Sales)").show()

  val countDistnctDirectorDF = moviesDF.select(countDistinct("Director"))

  countDistnctDirectorDF.show()
  moviesDF.select(approx_count_distinct("Director")).show()

  val statsUSRevDF = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  statsUSRevDF.show()

  //Compute  the average IMDB  rating  and  the average  US gross revenue PER  DIRECTOR
  val  aggByDirector = moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      avg("US_Gross").as("Avg_Gross")

    ).orderBy(col("Avg_Rating").desc_nulls_last)
  aggByDirector.show()




    moviesDF.select((col("US_Gross")+col("Worldwide_Gross")+col("US_DVD_Sales")).as("Total_Gross"))
      .select(sum("Total_Gross"))
      .show()
}

