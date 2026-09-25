import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16-Aggregations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read hospital revenue data
    val hospitalDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hospital_revenue.csv")

    println("\n=== Hospital Revenue Data ===")
    hospitalDF.show(false)

    println("\n=== Schema ===")
    hospitalDF.printSchema()

    // Basic aggregations
    println("\n=== Basic Aggregations ===")

    hospitalDF.agg(
      count("*").alias("total_records"),
      sum("revenue").alias("total_revenue"),
      avg("revenue").alias("average_revenue"),
      min("revenue").alias("minimum_revenue"),
      max("revenue").alias("maximum_revenue")
    ).show()

    // Department-wise revenue statistics
    println("\n=== Department-wise Revenue Statistics ===")

    val departmentStats = hospitalDF
      .groupBy("department")
      .agg(
        count("*").alias("visit_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy(desc("total_revenue"))

    departmentStats.show(false)

    // Group by multiple columns
    println("\n=== Department + Patient Type Statistics ===")

    val departmentPatientStats = hospitalDF
      .groupBy("department", "patient_type")
      .agg(
        count("*").alias("visit_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue")
      )
      .orderBy("department", "patient_type")

    departmentPatientStats.show(false)

    // HAVING-like filtering
    println("\n=== Departments with Total Revenue > 100000 ===")

    val highRevenueDepartments = departmentStats
      .filter(col("total_revenue") > 100000)

    highRevenueDepartments.show(false)

    spark.stop()
  }
}
