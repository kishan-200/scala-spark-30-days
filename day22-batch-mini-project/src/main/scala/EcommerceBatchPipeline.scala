import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EcommerceBatchPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22-Ecommerce-Batch-Pipeline")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // 1. READ RAW TRANSACTIONS
    // =========================================================

    println("\n=== RAW TRANSACTIONS ===")

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    transactions.show(false)

    // =========================================================
    // 2. READ CUSTOMER DATA
    // =========================================================

    println("\n=== CUSTOMERS ===")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    customers.show(false)

    // =========================================================
    // 3. READ PRODUCT DATA
    // =========================================================

    println("\n=== PRODUCTS ===")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    products.show(false)

    // =========================================================
    // 4. CLEAN INVALID TRANSACTIONS
    // =========================================================

    println("\n=== CLEAN TRANSACTIONS ===")

    val cleanTransactions = transactions
      .filter(col("quantity") > 0)
      .filter(col("customer_id").isNotNull)
      .filter(col("product_id").isNotNull)
      .filter(col("transaction_date").isNotNull)

    cleanTransactions.show(false)

    println(
      s"Valid transaction count: ${cleanTransactions.count()}"
    )

    // =========================================================
    // 5. JOIN TRANSACTIONS WITH CUSTOMERS
    // =========================================================

    println("\n=== AFTER CUSTOMER JOIN ===")

    val customerJoined = cleanTransactions
      .join(
        customers,
        cleanTransactions("customer_id") === customers("customer_id"),
        "inner"
      )
      .select(
        cleanTransactions("transaction_id"),
        cleanTransactions("customer_id"),
        customers("customer_name"),
        customers("city"),
        cleanTransactions("product_id"),
        cleanTransactions("quantity"),
        cleanTransactions("transaction_date")
      )

    customerJoined.show(false)

    // =========================================================
    // 6. JOIN WITH PRODUCTS
    // =========================================================

    println("\n=== AFTER PRODUCT JOIN ===")

    val enrichedTransactions = customerJoined
      .join(
        products,
        customerJoined("product_id") === products("product_id"),
        "inner"
      )
      .select(
        customerJoined("transaction_id"),
        customerJoined("customer_id"),
        customerJoined("customer_name"),
        customerJoined("city"),
        products("product_id"),
        products("product_name"),
        products("category"),
        products("price"),
        customerJoined("quantity"),
        customerJoined("transaction_date")
      )

    enrichedTransactions.show(false)

    // =========================================================
    // 7. CALCULATE REVENUE
    // =========================================================

    println("\n=== TRANSACTIONS WITH REVENUE ===")

    val transactionsWithRevenue = enrichedTransactions
      .withColumn(
        "revenue",
        col("quantity") * col("price")
      )

    transactionsWithRevenue.show(false)

    // =========================================================
    // 8. DAILY SALES AGGREGATION
    // =========================================================

    println("\n=== DAILY SALES ===")

    val dailySales = transactionsWithRevenue
      .groupBy(
        col("transaction_date"),
        col("category")
      )
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_transaction_value")
      )
      .orderBy(
        col("transaction_date"),
        col("category")
      )

    dailySales.show(false)

    // =========================================================
    // 9. WRITE PARTITIONED PARQUET
    // =========================================================

    println("\n=== WRITING PARTITIONED PARQUET ===")

    dailySales
      .write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .parquet("output/daily-sales")

    println(
      "Partitioned Parquet output written to: output/daily-sales"
    )

    // =========================================================
    // 10. READ OUTPUT BACK
    // =========================================================

    println("\n=== VERIFY PARQUET OUTPUT ===")

    val outputData = spark.read
      .parquet("output/daily-sales")

    outputData.show(false)

    println("\n=== OUTPUT SCHEMA ===")

    outputData.printSchema()

    spark.stop()
  }
}
