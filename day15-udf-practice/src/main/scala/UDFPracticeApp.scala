import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFPracticeApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UDF Practice")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("===== DAY 15: UDF PRACTICE =====")

    // --------------------------------------------------
    // 1. Read transaction data
    // --------------------------------------------------

    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_transactions.csv")

    println("\n===== TRANSACTION DATA =====")
    transactionsDF.show(false)

    println("\n===== SCHEMA =====")
    transactionsDF.printSchema()

    // --------------------------------------------------
    // 2. Scala UDF: Salary Band Classification
    // --------------------------------------------------

    val salaryBandUDF = udf { salary: Double =>
      if (salary >= 100000) "High"
      else if (salary >= 50000) "Medium"
      else "Low"
    }

    println("\n===== SALARY BAND UDF =====")

    val salaryExampleDF =
      transactionsDF.withColumn(
        "salary_band",
        salaryBandUDF(col("transaction_value"))
      )

    salaryExampleDF
      .select(
        "customer_id",
        "transaction_value",
        "salary_band"
      )
      .show(false)

    // --------------------------------------------------
    // 3. Customer Risk UDF
    // --------------------------------------------------

    val riskCategoryUDF = udf { value: Double =>
      if (value >= 100000) "High Risk"
      else if (value >= 50000) "Medium Risk"
      else if (value >= 10000) "Low Risk"
      else "Very Low Risk"
    }

    val riskDF =
      transactionsDF
        .withColumn(
          "risk_category",
          riskCategoryUDF(col("transaction_value"))
        )
        .withColumn(
          "transaction_value_in_thousands",
          round(col("transaction_value") / 1000, 2)
        )

    println("\n===== CUSTOMER RISK CATEGORY =====")

    riskDF
      .select(
        "transaction_id",
        "customer_id",
        "transaction_value",
        "transaction_value_in_thousands",
        "risk_category"
      )
      .show(false)

    // --------------------------------------------------
    // 4. Built-in Spark function comparison
    // --------------------------------------------------

    val builtInDF =
      transactionsDF.withColumn(
        "value_in_thousands",
        round(col("transaction_value") / 1000, 2)
      )

    println("\n===== BUILT-IN SPARK FUNCTION =====")

    builtInDF
      .select(
        "customer_id",
        "transaction_value",
        "value_in_thousands"
      )
      .show(false)

    println("\n===== UDF VS BUILT-IN FUNCTION =====")
    println("UDF:")
    println("- Custom business logic")
    println("- Useful when built-in Spark functions are insufficient")
    println("- Can introduce serialization and execution overhead")

    println("\nBuilt-in Spark function:")
    println("- Uses Spark's native expression system")
    println("- Generally easier for Spark to optimize")
    println("- Preferred when equivalent functionality exists")

    // --------------------------------------------------
    // 5. Register UDF with Spark Session / Catalog
    // --------------------------------------------------

    spark.udf.register(
      "customerRisk",
      (value: Double) => {
        if (value >= 100000) "High Risk"
        else if (value >= 50000) "Medium Risk"
        else if (value >= 10000) "Low Risk"
        else "Very Low Risk"
      }
    )

    println("\n===== REGISTERED UDF =====")
    println("UDF name: customerRisk")

    // --------------------------------------------------
    // 6. Temporary SQL View
    // --------------------------------------------------

    transactionsDF.createOrReplaceTempView("transactions")

    // --------------------------------------------------
    // 7. Use registered UDF in Spark SQL
    // --------------------------------------------------

    println("\n===== SPARK SQL WITH REGISTERED UDF =====")

    val sqlRiskDF =
      spark.sql(
        """
          |SELECT
          |  transaction_id,
          |  customer_id,
          |  transaction_value,
          |  customerRisk(transaction_value) AS risk_category
          |FROM transactions
          |ORDER BY transaction_value DESC
          |""".stripMargin
      )

    sqlRiskDF.show(false)

    // --------------------------------------------------
    // 8. Risk Category Summary
    // --------------------------------------------------

    println("\n===== RISK CATEGORY SUMMARY =====")

    sqlRiskDF
      .groupBy("risk_category")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // --------------------------------------------------
    // 9. Customer Risk Report
    // --------------------------------------------------

    println("\n===== CUSTOMER RISK REPORT =====")

    val customerRiskReport =
      spark.sql(
        """
          |SELECT
          |  customer_id,
          |  SUM(transaction_value) AS total_transaction_value,
          |  COUNT(*) AS transaction_records,
          |  customerRisk(SUM(transaction_value)) AS overall_risk
          |FROM transactions
          |GROUP BY customer_id
          |ORDER BY total_transaction_value DESC
          |""".stripMargin
      )

    customerRiskReport.show(false)

    // --------------------------------------------------
    // 10. UDF Summary
    // --------------------------------------------------

    println("\n===== UDF SUMMARY =====")
    println("1. Created a Scala UDF for salary band classification.")
    println("2. Created a customer risk classification UDF.")
    println("3. Added calculated columns using withColumn().")
    println("4. Compared UDF processing with a built-in Spark function.")
    println("5. Registered a UDF with the Spark session.")
    println("6. Used the registered UDF in Spark SQL.")
    println("7. Created a customer risk analytics report.")

    println("\n===== DAY 15 COMPLETE =====")

    spark.stop()
  }
}
