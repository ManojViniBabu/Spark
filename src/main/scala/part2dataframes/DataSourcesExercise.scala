package part2dataframes

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataSourcesExercise extends App {
  /*
   Read movies.json and write it as 3  different  things
   -->tab separated  csv  file
   -->snappy parquet
   -->table in postgres DB in public.movies

   */
  //Creating  a spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("DataSourcesExercise")
    .config("spark.master", "local")
    .getOrCreate()
  val moviesDF: DataFrame = spark.read.json(path="src/main/resources/data/movies.json")

  
  moviesDF.write
    .format(source="csv")
    .option("sep","\t")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save(path="src/main/resources/data/movies.csv")

  //Parquet
  moviesDF.write.save(path="src/main/resources/data/movies.parquet")

  //To DB
  //Reusabe  values
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"


  moviesDF.write
    .format( source="jdbc")
    .option("driver",driver)
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable","public.movies")
    .save()



  val moviesShowDF = spark.read
    .format( source="jdbc")
    .option("driver",driver)
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable","public.movies")
    .load()

  moviesShowDF.show()

}

