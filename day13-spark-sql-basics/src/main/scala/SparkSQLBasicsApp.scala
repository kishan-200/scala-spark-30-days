import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLBasicsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL Basics")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("===== DAY 13: SPARK SQL BASICS =====")

    // --------------------------------------------------
    // 1. CREATE DATAFRAME FROM CSV
    // --------------------------------------------------

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n===== CSV DATAFRAME =====")
    customersDF.show(false)

    // --------------------------------------------------
    // 2. INSPECT SCHEMA
    // --------------------------------------------------

    println("\n===== CSV SCHEMA =====")
    customersDF.printSchema()

    // --------------------------------------------------
    // 3. SELECT COLUMNS
    // --------------------------------------------------

    println("\n===== SELECTED COLUMNS =====")

    customersDF
      .select(
        "customer_id",
        "name",
        "city",
        "segment",
        "total_spend"
      )
      .show(false)

    // --------------------------------------------------
    // 4. FILTER COLUMNS
    // --------------------------------------------------

    println("\n===== PREMIUM CUSTOMERS =====")

    customersDF
      .filter(col("segment") === "Premium")
      .select(
        "customer_id",
        "name",
        "city",
        "total_spend"
      )
      .show(false)

    // --------------------------------------------------
    // 5. withColumn + EXPRESSION
    // --------------------------------------------------

    val customerAnalyticsDF =
      customersDF.withColumn(
        "average_order_value",
        round(col("total_spend") / col("orders"), 2)
      )

    println("\n===== withColumn: AVERAGE ORDER VALUE =====")

    customerAnalyticsDF
      .select(
        "customer_id",
        "name",
        "total_spend",
        "orders",
        "average_order_value"
      )
      .show(false)

    // --------------------------------------------------
    // 6. ANOTHER withColumn
    // --------------------------------------------------

    val enhancedCustomersDF =
      customerAnalyticsDF.withColumn(
        "customer_value",
        when(col("total_spend") >= 100000, "High")
          .when(col("total_spend") >= 50000, "Medium")
          .otherwise("Low")
      )

    println("\n===== CUSTOMER VALUE =====")

    enhancedCustomersDF
      .select(
        "customer_id",
        "name",
        "segment",
        "total_spend",
        "customer_value"
      )
      .show(false)

    // --------------------------------------------------
    // 7. CREATE DATAFRAME FROM JSON
    // --------------------------------------------------

    val preferencesDF = spark.read
      .option("multiLine", "true")
      .json("data/customer_preferences.json")

    println("\n===== JSON DATAFRAME =====")
    preferencesDF.show(false)

    println("\n===== JSON SCHEMA =====")
    preferencesDF.printSchema()

    // --------------------------------------------------
    // 8. TEMPORARY VIEW
    // --------------------------------------------------

    enhancedCustomersDF.createOrReplaceTempView("customers")

    preferencesDF.createOrReplaceTempView("customer_preferences")

    println("\n===== TEMPORARY VIEWS CREATED =====")
    println("customers")
    println("customer_preferences")

    // --------------------------------------------------
    // 9. BASIC SQL QUERY
    // --------------------------------------------------

    println("\n===== SQL: PREMIUM CUSTOMERS =====")

    val premiumCustomers =
      spark.sql(
        """
          |SELECT
          |  customer_id,
          |  name,
          |  city,
          |  total_spend
          |FROM customers
          |WHERE segment = 'Premium'
          |ORDER BY total_spend DESC
          |""".stripMargin
      )

    premiumCustomers.show(false)

    // --------------------------------------------------
    // 10. CITY ANALYTICS
    // --------------------------------------------------

    println("\n===== SQL: REVENUE BY CITY =====")

    val revenueByCity =
      spark.sql(
        """
          |SELECT
          |  city,
          |  COUNT(*) AS customer_count,
          |  SUM(total_spend) AS total_revenue,
          |  ROUND(AVG(total_spend), 2) AS average_spend
          |FROM customers
          |GROUP BY city
          |ORDER BY total_revenue DESC
          |""".stripMargin
      )

    revenueByCity.show(false)

    // --------------------------------------------------
    // 11. SEGMENT ANALYTICS
    // --------------------------------------------------

    println("\n===== SQL: ANALYTICS BY SEGMENT =====")

    val segmentAnalytics =
      spark.sql(
        """
          |SELECT
          |  segment,
          |  COUNT(*) AS customer_count,
          |  SUM(total_spend) AS total_revenue,
          |  ROUND(AVG(total_spend), 2) AS average_spend
          |FROM customers
          |GROUP BY segment
          |ORDER BY total_revenue DESC
          |""".stripMargin
      )

    segmentAnalytics.show(false)

    // --------------------------------------------------
    // 12. JOIN CSV AND JSON DATA
    // --------------------------------------------------

    println("\n===== SQL: CUSTOMER PREFERENCE ANALYTICS =====")

    val customerPreferenceAnalytics =
      spark.sql(
        """
          |SELECT
          |  c.customer_id,
          |  c.name,
          |  c.city,
          |  c.segment,
          |  c.total_spend,
          |  p.preferred_channel,
          |  p.loyalty_points
          |FROM customers c
          |JOIN customer_preferences p
          |  ON c.customer_id = p.customer_id
          |ORDER BY c.total_spend DESC
          |""".stripMargin
      )

    customerPreferenceAnalytics.show(false)

    // --------------------------------------------------
    // 13. CUSTOMER ANALYTICS REPORT
    // --------------------------------------------------

    println("\n===== CUSTOMER ANALYTICS REPORT =====")

    val report =
      spark.sql(
        """
          |SELECT
          |  segment,
          |  city,
          |  COUNT(*) AS customers,
          |  SUM(total_spend) AS revenue,
          |  ROUND(AVG(total_spend), 2) AS avg_spend,
          |  ROUND(AVG(orders), 2) AS avg_orders
          |FROM customers
          |GROUP BY segment, city
          |ORDER BY revenue DESC
          |""".stripMargin
      )

    report.show(false)

    // --------------------------------------------------
    // 14. DATAFRAME API vs SQL
    // --------------------------------------------------

    println("\n===== DATAFRAME API + SPARK SQL =====")

    println("DataFrame API:")
    println("- select()")
    println("- filter()")
    println("- withColumn()")
    println("- column expressions")

    println("\nSpark SQL:")
    println("- Temporary views")
    println("- SELECT")
    println("- WHERE")
    println("- GROUP BY")
    println("- ORDER BY")
    println("- JOIN")

    println("\n===== DAY 13 COMPLETE =====")

    spark.stop()
  }
}
