import org.apache.spark.sql.SparkSession

object SparkFirstApp {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Spark First Application")
      .master("local[4]")
      .getOrCreate()

    // Get SparkContext from SparkSession
    val sc = spark.sparkContext

    println("===== SPARK APPLICATION =====")
    println("Application Name: " + spark.sparkContext.appName)
    println("Master: " + spark.sparkContext.master)

    // Read text file using SparkContext
    val lines = sc.textFile("data/input.txt")

    println("\n===== FILE CONTENTS =====")

    lines.collect().foreach(println)

    // Stop Spark
    spark.stop()
  }
}
