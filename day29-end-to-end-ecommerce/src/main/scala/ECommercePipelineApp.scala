import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.storage.StorageLevel

object ECommercePipelineApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day29-End-to-End-E-Commerce")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("========================================")
    println("RAW -> CLEAN PIPELINE")
    println("========================================")

    // -----------------------------
    // 1. Read raw events using RDD
    // -----------------------------
    val rawRDD = spark.sparkContext.textFile("data/ecommerce_events.txt")

    println(s"Raw event count: ${rawRDD.count()}")

    // -----------------------------
    // 2. Clean and validate records
    // -----------------------------
    val cleanedRDD = rawRDD
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter { line =>
        val parts = line.split(",")
        parts.length == 7
      }

    println(s"Valid event count: ${cleanedRDD.count()}")

    // -----------------------------
    // 3. Convert to DataFrame
    // -----------------------------
    import spark.implicits._

    val transactionsDF = cleanedRDD
      .map { line =>
        val parts = line.split(",")

        (
          parts(0),
          parts(1),
          parts(2),
          parts(3),
          parts(4).toInt,
          parts(5).toDouble,
          parts(6)
        )
      }
      .toDF(
        "transaction_id",
        "customer_id",
        "product_id",
        "category",
        "quantity",
        "amount",
        "event_time"
      )

    // -----------------------------
    // 4. Cache cleaned data
    // -----------------------------
    transactionsDF.persist(StorageLevel.MEMORY_ONLY)

    println("Cleaned transaction data:")

    transactionsDF.show(false)

    // -----------------------------
    // 5. Basic validation
    // -----------------------------
    println("Total cleaned transactions:")
    println(transactionsDF.count())

    println("Transactions by category:")

    transactionsDF
      .groupBy("category")
      .agg(
        F.count("*").alias("transaction_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("amount").alias("total_amount")
      )
      .show(false)

    transactionsDF.unpersist()

    println("========================================")
    println("RAW -> CLEAN COMPLETED")
    println("========================================")

    spark.stop()
  }
}
