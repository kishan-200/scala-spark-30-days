import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // LOAD DATA
    // =========================================================

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")

    println("\n=== CUSTOMERS ===")
    customers.show(false)

    println("\n=== ORDERS ===")
    orders.show(false)

    println("\n=== PAYMENTS ===")
    payments.show(false)

    // =========================================================
    // INNER JOIN
    // =========================================================

    println("\n=== INNER JOIN: ORDERS + CUSTOMERS ===")

    val innerJoin = orders
      .join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "inner"
      )
      .select(
        orders("order_id"),
        orders("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("amount")
      )

    innerJoin.show(false)

    // =========================================================
    // LEFT JOIN
    // =========================================================

    println("\n=== LEFT JOIN: ALL ORDERS + CUSTOMER DETAILS ===")

    val leftJoin = orders
      .join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "left"
      )
      .select(
        orders("order_id"),
        orders("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("product"),
        orders("amount")
      )

    leftJoin.show(false)

    // =========================================================
    // NULL HANDLING AFTER LEFT JOIN
    // =========================================================

    println("\n=== NULL HANDLING AFTER LEFT JOIN ===")

    val leftJoinWithNullHandling = leftJoin
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Unknown Customer"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("Unknown City"))
      )

    leftJoinWithNullHandling.show(false)

    // =========================================================
    // RIGHT JOIN
    // =========================================================

    println("\n=== RIGHT JOIN: ORDERS + CUSTOMERS ===")

    val rightJoin = orders
      .join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "right"
      )
      .select(
        customers("customer_id").alias("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("product"),
        orders("amount")
      )

    rightJoin.show(false)

    // =========================================================
    // FULL OUTER JOIN
    // =========================================================

    println("\n=== FULL OUTER JOIN: ORDERS + CUSTOMERS ===")

    val fullJoin = orders
      .join(
        customers,
        orders("customer_id") === customers("customer_id"),
        "full"
      )
      .select(
        coalesce(
          orders("customer_id"),
          customers("customer_id")
        ).alias("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("product"),
        orders("amount")
      )

    fullJoin.show(false)

    // =========================================================
    // ALIASES TO HANDLE AMBIGUOUS COLUMN NAMES
    // =========================================================

    println("\n=== JOIN USING ALIASES ===")

    val o = orders.alias("o")
    val c = customers.alias("c")

    val aliasedJoin = o
      .join(
        c,
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id").alias("order_customer_id"),
        col("c.customer_id").alias("customer_customer_id"),
        col("c.customer_name"),
        col("o.product"),
        col("o.amount")
      )

    aliasedJoin.show(false)

    // =========================================================
    // ORDERS + CUSTOMERS + PAYMENTS
    // =========================================================

    println("\n=== ORDERS + CUSTOMERS + PAYMENTS ===")

    val orderCustomer = orders
      .alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )

    val finalReport = orderCustomer
      .join(
        payments.alias("p"),
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount"),
        col("p.payment_method"),
        col("p.payment_status")
      )

    finalReport.show(false)

    // =========================================================
    // SHUFFLE SORT MERGE JOIN
    // =========================================================

    println("\n=== JOIN EXECUTION PLAN ===")

    finalReport.explain(true)

    spark.stop()
  }
}
