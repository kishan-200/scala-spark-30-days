import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCatalogApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Spark-Catalog")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // 1. LIST DATABASES
    // =========================================================

    println("\n=== DATABASES BEFORE CREATION ===")

    spark.catalog.listDatabases().show(false)

    // =========================================================
    // 2. CREATE ANALYTICS DATABASE
    // =========================================================

    spark.sql("""
      CREATE DATABASE IF NOT EXISTS hotel_analytics
    """)

    println("\n=== DATABASES AFTER CREATION ===")

    spark.catalog.listDatabases().show(false)

    // =========================================================
    // 3. USE DATABASE
    // =========================================================

    spark.sql("""
      USE hotel_analytics
    """)

    println("\nCurrent database:")
    println(spark.catalog.currentDatabase)

    // =========================================================
    // 4. READ HOTEL BOOKING DATA
    // =========================================================

    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hotel_bookings.csv")

    println("\n=== HOTEL BOOKINGS ===")
    bookings.show(false)

    println("\n=== BOOKING SCHEMA ===")
    bookings.printSchema()

    // =========================================================
    // 5. CREATE TEMPORARY VIEW
    // =========================================================

    bookings.createOrReplaceTempView("bookings_temp")

    println("\n=== TEMPORARY VIEW CREATED ===")

    println(
      s"Temporary view exists: ${spark.catalog.tableExists("bookings_temp")}"
    )

    // =========================================================
    // 6. QUERY TEMPORARY VIEW
    // =========================================================

    println("\n=== CONFIRMED BOOKINGS ===")

    val confirmedBookings = spark.sql("""
      SELECT
        booking_id,
        customer_id,
        hotel,
        city,
        room_type,
        amount,
        status
      FROM bookings_temp
      WHERE status = 'Confirmed'
    """)

    confirmedBookings.show(false)

    // =========================================================
    // 7. CREATE PERMANENT TABLE
    // =========================================================

    bookings.write
      .mode("overwrite")
      .saveAsTable("hotel_bookings")

    println("\n=== TABLE CREATED ===")

    // =========================================================
    // 8. LIST TABLES
    // =========================================================

    println("\n=== TABLES IN HOTEL_ANALYTICS ===")

    spark.catalog.listTables("hotel_analytics").show(false)

    // =========================================================
    // 9. QUERY PERMANENT TABLE
    // =========================================================

    println("\n=== BOOKINGS FROM PERMANENT TABLE ===")

    spark.sql("""
      SELECT
        booking_id,
        hotel,
        city,
        amount,
        status
      FROM hotel_bookings
      ORDER BY amount DESC
    """).show(false)

    // =========================================================
    // 10. HOTEL REVENUE ANALYSIS
    // =========================================================

    println("\n=== HOTEL REVENUE ===")

    spark.sql("""
      SELECT
        hotel,
        city,
        COUNT(*) AS total_bookings,
        SUM(amount) AS total_revenue,
        AVG(amount) AS average_booking_amount
      FROM hotel_bookings
      WHERE status != 'Cancelled'
      GROUP BY hotel, city
      ORDER BY total_revenue DESC
    """).show(false)

    // =========================================================
    // 11. INSPECT TABLE COLUMNS
    // =========================================================

    println("\n=== TABLE COLUMNS ===")

    spark.catalog
      .listColumns("hotel_analytics", "hotel_bookings")
      .show(false)

    // =========================================================
    // 12. TABLE EXISTS CHECK
    // =========================================================

    println("\n=== TABLE EXISTS ===")

    println(
      s"hotel_bookings exists: ${
        spark.catalog.tableExists("hotel_analytics", "hotel_bookings")
      }"
    )

    // =========================================================
    // 13. DATABASE TABLE COUNT
    // =========================================================

    println("\n=== TABLE COUNT ===")

    val tableCount = spark.catalog
      .listTables("hotel_analytics")
      .count()

    println(s"Tables in hotel_analytics: $tableCount")

    // =========================================================
    // 14. EXPLAIN QUERY
    // =========================================================

    println("\n=== QUERY PLAN ===")

    spark.sql("""
      SELECT
        hotel,
        SUM(amount) AS revenue
      FROM hotel_bookings
      WHERE status != 'Cancelled'
      GROUP BY hotel
    """).explain(true)

    spark.stop()
  }
}
