import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileFormatsOutputApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-File-Formats-Output")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // 1. READ CSV
    // =========================================================

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/daily_sales.csv")

    println("\n=== SALES DATA ===")
    sales.show(false)

    println("\n=== SALES SCHEMA ===")
    sales.printSchema()

    // =========================================================
    // 2. CREATE DATE COLUMNS
    // =========================================================

    val salesWithDate = sales
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    println("\n=== SALES WITH DATE PARTITIONS ===")
    salesWithDate.show(false)

    // =========================================================
    // 3. WRITE CSV
    // =========================================================

    salesWithDate
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv")

    println("\nCSV output written to output/csv")

    // =========================================================
    // 4. WRITE JSON
    // =========================================================

    salesWithDate
      .write
      .mode("overwrite")
      .json("output/json")

    println("JSON output written to output/json")

    // =========================================================
    // 5. WRITE PARQUET
    // =========================================================

    salesWithDate
      .write
      .mode("overwrite")
      .parquet("output/parquet")

    println("Parquet output written to output/parquet")

    // =========================================================
    // 6. WRITE PARTITIONED PARQUET
    // =========================================================

    salesWithDate
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/partitioned-sales")

    println("Partitioned Parquet output written to output/partitioned-sales")

    // =========================================================
    // 7. REPARTITION BEFORE WRITING
    // =========================================================

    val repartitionedSales = salesWithDate
      .repartition(2)

    println(
      s"\nPartitions after repartition: ${repartitionedSales.rdd.getNumPartitions}"
    )

    repartitionedSales
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/repartitioned-sales")

    println("Repartitioned output written to output/repartitioned-sales")

    // =========================================================
    // 8. READ PARQUET BACK
    // =========================================================

    val parquetData = spark.read
      .parquet("output/partitioned-sales")

    println("\n=== READ PARQUET DATA ===")
    parquetData.show(false)

    // =========================================================
    // 9. FILE FORMAT COMPARISON
    // =========================================================

    println("\n=== FILE FORMAT SUMMARY ===")

    println(
      """
        |CSV     -> Human-readable tabular format
        |JSON    -> Semi-structured record format
        |Parquet -> Columnar format optimized for analytics
        |""".stripMargin
    )

    spark.stop()
  }
}
