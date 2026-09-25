import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastJoinApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Broadcast-Join")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =========================================================
    // LOAD LARGE FACT DATA
    // =========================================================

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    // =========================================================
    // LOAD SMALL REFERENCE DATA
    // =========================================================

    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/branch_master.csv")

    println("\n=== TRANSACTIONS ===")
    transactions.show(false)

    println("\n=== BRANCH MASTER ===")
    branches.show(false)

    println("\n=== TRANSACTION SCHEMA ===")
    transactions.printSchema()

    println("\n=== BRANCH MASTER SCHEMA ===")
    branches.printSchema()

    // =========================================================
    // BROADCAST JOIN
    // =========================================================

    println("\n=== BROADCAST JOIN ===")

    val broadcastResult = transactions
      .join(
        broadcast(branches),
        transactions("branch_id") === branches("branch_id"),
        "left"
      )
      .select(
        transactions("transaction_id"),
        transactions("account_id"),
        transactions("branch_id"),
        branches("branch_name"),
        branches("city"),
        branches("region"),
        transactions("amount"),
        transactions("transaction_type")
      )

    broadcastResult.show(false)

    // =========================================================
    // BRANCH-WISE TRANSACTION METRICS
    // =========================================================

    println("\n=== BRANCH-WISE TRANSACTION METRICS ===")

    val branchMetrics = broadcastResult
      .groupBy(
        "branch_id",
        "branch_name",
        "city",
        "region"
      )
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        max("amount").alias("maximum_amount"),
        min("amount").alias("minimum_amount")
      )
      .orderBy(desc("total_amount"))

    branchMetrics.show(false)

    // =========================================================
    // BROADCAST JOIN EXECUTION PLAN
    // =========================================================

    println("\n=== BROADCAST JOIN EXECUTION PLAN ===")

    broadcastResult.explain(true)

    // =========================================================
    // COMPARISON: NORMAL JOIN
    // =========================================================

    println("\n=== NORMAL JOIN EXECUTION PLAN ===")

    val normalJoin = transactions
      .join(
        branches,
        transactions("branch_id") === branches("branch_id"),
        "left"
      )
      .select(
        transactions("transaction_id"),
        transactions("branch_id"),
        branches("branch_name"),
        transactions("amount")
      )

    normalJoin.explain(true)

    spark.stop()
  }
}
