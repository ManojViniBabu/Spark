package part2dataframes

import org.apache.spark.sql.SparkSession

//combine data from  multiple data frame
//WIDE TRANSFORMATION -expensive  read ie, inorder to scan the entire dataframe spark
//scans entire dataframe - shuffling are  expensive

object  Joins extends App {
  val  spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()



}

