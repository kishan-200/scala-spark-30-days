import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17-Window-Functions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // PART 1: STUDENT RANKING
    // =========================================================

    val students = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/students.csv")

    println("\n=== Student Data ===")
    students.show(false)

    println("\n=== Student Schema ===")
    students.printSchema()

    // Window partitioned by course and ordered by marks descending
    val studentWindow =
      Window
        .partitionBy("course")
        .orderBy(col("marks").desc)

    // ---------------------------------------------------------
    // ROW_NUMBER
    // ---------------------------------------------------------

    println("\n=== ROW_NUMBER ===")

    val withRowNumber = students
      .withColumn("row_number", row_number().over(studentWindow))

    withRowNumber
      .orderBy("course", "row_number")
      .show(false)

    // ---------------------------------------------------------
    // RANK
    // ---------------------------------------------------------

    println("\n=== RANK ===")

    val withRank = students
      .withColumn("rank", rank().over(studentWindow))

    withRank
      .orderBy("course", "rank")
      .show(false)

    // ---------------------------------------------------------
    // DENSE_RANK
    // ---------------------------------------------------------

    println("\n=== DENSE_RANK ===")

    val withDenseRank = students
      .withColumn("dense_rank", dense_rank().over(studentWindow))

    withDenseRank
      .orderBy("course", "dense_rank")
      .show(false)

    // ---------------------------------------------------------
    // TOP 3 STUDENTS PER COURSE
    // ---------------------------------------------------------

    println("\n=== TOP 3 STUDENTS PER COURSE ===")

    val top3Students = students
      .withColumn("rank", rank().over(studentWindow))
      .filter(col("rank") <= 3)

    top3Students
      .orderBy("course", "rank")
      .show(false)

    // =========================================================
    // PART 2: CUSTOMER POLICIES
    // =========================================================

    val policies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_policies.csv")

    println("\n=== Customer Policy Data ===")
    policies.show(false)

    // Convert policy_date to a proper date
    val policiesWithDate = policies
      .withColumn(
        "policy_date",
        to_date(col("policy_date"), "yyyy-MM-dd")
      )

    // Window for each customer ordered by policy date descending
    val latestPolicyWindow =
      Window
        .partitionBy("customer_id")
        .orderBy(col("policy_date").desc)

    // ---------------------------------------------------------
    // LATEST POLICY PER CUSTOMER
    // ---------------------------------------------------------

    println("\n=== LATEST POLICY PER CUSTOMER ===")

    val latestPolicies = policiesWithDate
      .withColumn(
        "row_number",
        row_number().over(latestPolicyWindow)
      )
      .filter(col("row_number") === 1)
      .drop("row_number")

    latestPolicies
      .orderBy("customer_id")
      .show(false)

    // =========================================================
    // PART 3: LAG AND LEAD
    // =========================================================

    val customerHistoryWindow =
      Window
        .partitionBy("customer_id")
        .orderBy("policy_date")

    println("\n=== LAG ===")

    val withLag = policiesWithDate
      .withColumn(
        "previous_premium",
        lag("premium", 1).over(customerHistoryWindow)
      )

    withLag
      .orderBy("customer_id", "policy_date")
      .show(false)

    println("\n=== LEAD ===")

    val withLead = policiesWithDate
      .withColumn(
        "next_premium",
        lead("premium", 1).over(customerHistoryWindow)
      )

    withLead
      .orderBy("customer_id", "policy_date")
      .show(false)

    // ---------------------------------------------------------
    // LAG + LEAD TOGETHER
    // ---------------------------------------------------------

    println("\n=== LAG AND LEAD TOGETHER ===")

    val policyHistory = policiesWithDate
      .withColumn(
        "previous_premium",
        lag("premium", 1).over(customerHistoryWindow)
      )
      .withColumn(
        "next_premium",
        lead("premium", 1).over(customerHistoryWindow)
      )

    policyHistory
      .orderBy("customer_id", "policy_date")
      .show(false)

    spark.stop()
  }
}
