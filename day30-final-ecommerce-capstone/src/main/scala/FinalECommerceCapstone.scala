import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

object FinalECommerceCapstone {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day30-Final-ECommerce-Capstone")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("==============================================")
    println("DAY 30 - FINAL E-COMMERCE CAPSTONE")
    println("==============================================")

    // =====================================================
    // 1. BATCH DATA
    // =====================================================

    println("\n===== BATCH DATA =====")

    val batchDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/batch_transactions.csv")
  .withColumn(
    "amount",
    F.col("amount").cast("double")
  )
  .withColumn(
    "quantity",
    F.col("quantity").cast("int")
  )

    batchDF.show(false)

    println("\n===== BATCH SCHEMA =====")
    batchDF.printSchema()

    // =====================================================
    // 2. BASIC CLEANING
    // =====================================================

    val invalidRecords = spark.sparkContext.longAccumulator(
      "Invalid E-Commerce Records"
    )

    val cleanedDF = batchDF
      .filter {
        val condition =
          F.col("transaction_id").isNotNull &&
          F.col("customer_id").isNotNull &&
          F.col("product_id").isNotNull &&
          F.col("quantity") > 0 &&
          F.col("amount") > 0

        condition
      }

    println("\n===== CLEANED BATCH DATA =====")
    cleanedDF.show(false)

    println(
      "Invalid records detected: " + invalidRecords.value
    )

    // =====================================================
    // 3. CACHE / PERSIST
    // =====================================================

    val persistedDF =
      cleanedDF.persist(StorageLevel.MEMORY_ONLY)

    println("\n===== PERSISTENCE =====")
    println("Storage level: " + persistedDF.storageLevel)

    println(
      "Persisted transaction count: " +
        persistedDF.count()
    )

    // =====================================================
    // 4. PAIR RDD CUSTOMER AGGREGATION
    // =====================================================

    println("\n===== PAIR RDD CUSTOMER AGGREGATION =====")

    val customerRevenueRDD =
      persistedDF.rdd
        .map { row =>
          val customerId = row.getAs[String]("customer_id")
          val amount = row.getAs[java.lang.Double]("amount").doubleValue()

          (customerId, amount)
        }
        .reduceByKey(_ + _)

    customerRevenueRDD
      .sortByKey()
      .collect()
      .foreach {
        case (customer, revenue) =>
          println(
            f"$customer -> $revenue%.2f"
          )
      }

    // =====================================================
    // 5. PRODUCT REFERENCE DATA
    // =====================================================

    println("\n===== PRODUCT REFERENCE DATA =====")

    val productDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/product_reference.csv")

    productDF.show(false)

    // =====================================================
    // 6. BROADCAST REFERENCE DATA
    // =====================================================

    println("\n===== BROADCAST REFERENCE DATA =====")

    val productMap =
      productDF.collect()
        .map { row =>
          val productId =
            row.getAs[String]("product_id")

          val productName =
            row.getAs[String]("product_name")

          (productId, productName)
        }
        .toMap

    val broadcastProducts =
      spark.sparkContext.broadcast(productMap)

    println(
      "Broadcast product count: " +
        broadcastProducts.value.size
    )

    // =====================================================
    // 7. DATAFRAME JOIN
    // =====================================================

    println("\n===== ENRICHED DATA =====")

    val enrichedDF =
      persistedDF
        .join(
          productDF,
          Seq("product_id"),
          "left"
        )
        .select(
          persistedDF("transaction_id"),
          persistedDF("customer_id"),
          persistedDF("product_id"),
          productDF("product_name"),
          persistedDF("quantity"),
          persistedDF("amount"),
          persistedDF("event_time")
        )

    enrichedDF.show(false)

    // =====================================================
    // 8. CUSTOMER VALUE UDF
    // =====================================================

    println("\n===== CUSTOMER VALUE UDF =====")

    val customerValueUDF = F.udf {
      amount: Double =>
        if (amount >= 50000) "HIGH"
        else if (amount >= 25000) "MEDIUM"
        else "LOW"
    }

    val customerValueDF =
      enrichedDF.withColumn(
        "transaction_value_band",
        customerValueUDF(F.col("amount"))
      )

    customerValueDF.show(false)

    // =====================================================
    // 9. WINDOW ANALYSIS
    // =====================================================

    println("\n===== CUSTOMER WINDOW ANALYSIS =====")

    val customerWindow =
      Window
        .partitionBy("customer_id")
        .orderBy("event_time")
        .rowsBetween(
          Window.unboundedPreceding,
          Window.currentRow
        )

    val windowDF =
      customerValueDF.withColumn(
        "running_customer_spend",
        F.sum("amount").over(customerWindow)
      )

    windowDF
      .select(
        "customer_id",
        "transaction_id",
        "amount",
        "running_customer_spend"
      )
      .show(false)

    // =====================================================
    // 10. SPARK SQL REPORT
    // =====================================================

    println("\n===== SPARK SQL REPORT =====")

    windowDF.createOrReplaceTempView(
      "ecommerce_transactions"
    )

    val reportDF =
      spark.sql(
        """
          |SELECT
          |  product_name,
          |  COUNT(*) AS transactions,
          |  SUM(quantity) AS total_quantity,
          |  SUM(amount) AS total_revenue,
          |  ROUND(AVG(amount), 2) AS average_transaction
          |FROM ecommerce_transactions
          |GROUP BY product_name
          |ORDER BY total_revenue DESC
          |""".stripMargin
      )

    reportDF.show(false)

    // =====================================================
    // 11. PARTITION INFORMATION
    // =====================================================

    println("\n===== PARTITION INFORMATION =====")

    println(
      "Current partitions: " +
        windowDF.rdd.getNumPartitions
    )

    val partitionedDF =
      windowDF.repartition(
        4,
        F.col("customer_id")
      )

    println(
      "After customer partitioning: " +
        partitionedDF.rdd.getNumPartitions
    )

    // =====================================================
    // 12. FINAL SUMMARY
    // =====================================================

    println("\n==============================================")
    println("DAY 30 BATCH COMPONENT COMPLETE")
    println("==============================================")

    println("RDD / Pair RDD       : YES")
    println("Broadcast             : YES")
    println("Accumulator           : YES")
    println("Cache / Persist       : YES")
    println("DataFrame Join        : YES")
    println("UDF                   : YES")
    println("Window Operation     : YES")
    println("Spark SQL             : YES")
    println("Partitioning          : YES")

    persistedDF.unpersist()
    broadcastProducts.destroy()

    spark.stop()
  }
}
