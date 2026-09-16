import org.apache.spark.{SparkConf, SparkContext}

object PairRDDApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Pair RDD Application")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 9: PAIR RDD =====")

    // ============================================================
    // 1. READ SALES DATA
    // ============================================================

    val sales = sc.textFile("data/sales.txt", 4)

    println("\n===== SALES DATA =====")
    sales.collect().foreach(println)

    // ============================================================
    // 2. CREATE PRODUCT REVENUE PAIR RDD
    // ============================================================

    val productRevenue = sales.map { line =>
      val parts = line.split(",")
      (parts(0), parts(2).toDouble)
    }

    println("\n===== PRODUCT REVENUE PAIR RDD =====")
    productRevenue.collect().foreach(println)

    // ============================================================
    // 3. reduceByKey - REVENUE BY PRODUCT
    // ============================================================

    val revenueByProduct =
      productRevenue.reduceByKey(_ + _)

    println("\n===== reduceByKey: REVENUE BY PRODUCT =====")
    revenueByProduct
      .collect()
      .sortBy(-_._2)
      .foreach(println)

    // ============================================================
    // 4. groupByKey - REVENUE BY PRODUCT
    // ============================================================

    val groupedRevenueByProduct =
      productRevenue.groupByKey()

    println("\n===== groupByKey: REVENUE BY PRODUCT =====")

    groupedRevenueByProduct
      .mapValues(values => values.sum)
      .collect()
      .sortBy(-_._2)
      .foreach(println)

    // ============================================================
    // 5. mapValues DEMONSTRATION
    // ============================================================

    val productRevenueInThousands =
      revenueByProduct.mapValues(_ / 1000)

    println("\n===== mapValues: REVENUE IN THOUSANDS =====")

    productRevenueInThousands
      .collect()
      .sortBy(-_._2)
      .foreach {
        case (product, revenue) =>
          println(product + " -> " + revenue + " thousand")
      }

    // ============================================================
    // 6. REVENUE BY DEPARTMENT
    // ============================================================

    val departmentRevenue = sales.map { line =>
      val parts = line.split(",")
      (parts(1), parts(2).toDouble)
    }

    val revenueByDepartment =
      departmentRevenue.reduceByKey(_ + _)

    println("\n===== REVENUE BY DEPARTMENT =====")

    revenueByDepartment
      .collect()
      .sortBy(-_._2)
      .foreach(println)

    // ============================================================
    // 7. reduceByKey vs groupByKey
    // ============================================================

    println("\n===== reduceByKey vs groupByKey =====")

    println("reduceByKey:")
    println("- Performs local/partial aggregation before shuffle.")
    println("- Transfers less data across partitions.")
    println("- Generally preferred for aggregation.")

    println("\ngroupByKey:")
    println("- Groups all values for each key.")
    println("- More data may be transferred during shuffle.")
    println("- Can use more memory for grouped values.")

    // ============================================================
    // 8. BANK TRANSACTION AGGREGATION
    // ============================================================

    val transactions =
      sc.textFile("data/bank_transactions.txt", 4)

    val accountTransactions = transactions.map { line =>
      val parts = line.split(",")
      (parts(1), parts(2).toDouble)
    }

    val totalByAccount =
      accountTransactions.reduceByKey(_ + _)

    println("\n===== BANK TRANSACTIONS BY ACCOUNT =====")

    totalByAccount
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, amount) =>
          println(account + " -> " + amount)
      }

    // ============================================================
    // 9. PARTITION INFORMATION
    // ============================================================

    println("\n===== PARTITION INFORMATION =====")

    println("Sales partitions: " +
      sales.getNumPartitions)

    println("Product Pair RDD partitions: " +
      productRevenue.getNumPartitions)

    println("reduceByKey partitions: " +
      revenueByProduct.getNumPartitions)

    println("Bank transaction partitions: " +
      transactions.getNumPartitions)

    println("\n===== PAIR RDD OPERATIONS COMPLETED =====")

    sc.stop()
  }
}
