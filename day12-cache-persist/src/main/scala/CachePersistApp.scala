import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CachePersistApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Cache and Persist")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 12: CACHE AND PERSIST =====")

    // --------------------------------------------------
    // 1. LOAD RAW TRANSACTIONS
    // --------------------------------------------------

    val rawTransactions =
      sc.textFile("data/transactions.txt", 4)

    println("\n===== RAW TRANSACTIONS =====")
    println("Raw partitions: " +
      rawTransactions.getNumPartitions)

    // --------------------------------------------------
    // 2. CLEAN THE DATASET
    // --------------------------------------------------

    val cleanedTransactions = rawTransactions
      .filter { line =>
        val parts = line.split(",")

        parts.length == 6 &&
        parts(4) != "invalid" &&
        parts(5) == "completed"
      }
      .map { line =>
        val parts = line.split(",")

        (
          parts(0),
          parts(1),
          parts(2),
          parts(3),
          parts(4).toDouble,
          parts(5)
        )
      }

    // --------------------------------------------------
    // 3. CACHE THE CLEANED RDD
    // --------------------------------------------------

    cleanedTransactions.cache()

    println("\n===== CACHE =====")
    println("Cleaned RDD has been cached.")
    println("Storage level: " +
      cleanedTransactions.getStorageLevel)

    // --------------------------------------------------
    // 4. FIRST ACTION - REPORT 1
    // --------------------------------------------------

    println("\n===== REPORT 1: TOTAL REVENUE =====")

    val totalRevenue =
      cleanedTransactions
        .map(_._5)
        .reduce(_ + _)

    println("Total revenue: " + totalRevenue)

    // --------------------------------------------------
    // 5. SECOND ACTION - REPORT 2
    // --------------------------------------------------

    println("\n===== REPORT 2: REVENUE BY ACCOUNT =====")

    val revenueByAccount =
      cleanedTransactions
        .map(transaction =>
          (transaction._2, transaction._5)
        )
        .reduceByKey(_ + _)

    revenueByAccount
      .collect()
      .sortBy(_._1)
      .foreach(println)

    // --------------------------------------------------
    // 6. THIRD ACTION - REPORT 3
    // --------------------------------------------------

    println("\n===== REPORT 3: REVENUE BY CATEGORY =====")

    val revenueByCategory =
      cleanedTransactions
        .map(transaction =>
          (transaction._4, transaction._5)
        )
        .reduceByKey(_ + _)

    revenueByCategory
      .collect()
      .sortBy(_._1)
      .foreach(println)

    // --------------------------------------------------
    // 7. CACHE REUSE
    // --------------------------------------------------

    println("\n===== CACHED RDD REUSE =====")

    println(
      "The same cleaned RDD was reused for three reports."
    )

    println(
      "Without caching, Spark may recompute the cleaning "
        + "lineage for each action."
    )

    println(
      "With caching, the cleaned RDD can be reused "
        + "after it has been materialized."
    )

    // --------------------------------------------------
    // 8. CACHE VS PERSIST
    // --------------------------------------------------

    println("\n===== CACHE VS PERSIST =====")

    println("cache():")
    println("- Convenience method for caching an RDD.")
    println("- Uses the default MEMORY_ONLY storage level.")

    println("\npersist():")
    println("- Allows an explicit storage level.")
    println("- Example: MEMORY_AND_DISK.")

    println(
      "\nBoth cache() and persist() store computed partitions "
        + "for reuse."
    )

    // --------------------------------------------------
    // 9. DIFFERENT STORAGE LEVELS
    // --------------------------------------------------

    println("\n===== STORAGE LEVELS =====")

    println("MEMORY_ONLY")
    println("- Stores partitions in memory.")
    println("- If a partition does not fit, it can be recomputed.")

    println("\nMEMORY_AND_DISK")
    println("- Stores partitions in memory when possible.")
    println("- Remaining partitions can be stored on disk.")

    println("\nDISK_ONLY")
    println("- Stores partitions only on disk.")
    println("- Useful when memory is limited.")

    // --------------------------------------------------
    // 10. PERSIST EXPERIMENT
    // --------------------------------------------------

    val persistedTransactions =
      rawTransactions
        .filter { line =>
          val parts = line.split(",")

          parts.length == 6 &&
          parts(4) != "invalid" &&
          parts(5) == "completed"
        }
        .map { line =>
          val parts = line.split(",")

          (
            parts(0),
            parts(1),
            parts(2),
            parts(3),
            parts(4).toDouble,
            parts(5)
          )
        }
        .persist(StorageLevel.MEMORY_AND_DISK)

    val persistedCount =
      persistedTransactions.count()

    println("\n===== PERSIST EXPERIMENT =====")
    println("Storage level: " +
      persistedTransactions.getStorageLevel)

    println("Persisted cleaned records: " +
      persistedCount)

    // --------------------------------------------------
    // 11. WHEN CACHING CAN HURT
    // --------------------------------------------------

    println("\n===== WHEN CACHING CAN HURT =====")

    println("- Caching consumes executor memory.")
    println("- Caching a dataset used only once may add overhead.")
    println("- Very large datasets may cause memory pressure.")
    println("- Excessive cached datasets can cause eviction.")
    println("- Poor caching decisions can increase GC pressure.")

    // --------------------------------------------------
    // 12. OPTIMIZATION SCENARIO
    // --------------------------------------------------

    println("\n===== OPTIMIZATION SCENARIO =====")

    println("Problem:")
    println("- Transaction data must be cleaned before reporting.")
    println("- Three reports reuse the same cleaned dataset.")
    println("- Recomputing the cleaning steps for every report")
    println("  can waste computation.")

    println("\nSolution:")
    println("- Clean the transaction RDD once.")
    println("- Cache the cleaned RDD.")
    println("- Reuse it for multiple reports.")

    println("\nReports using cached data:")
    println("1. Total Revenue")
    println("2. Revenue by Account")
    println("3. Revenue by Category")

    // --------------------------------------------------
    // 13. CLEANUP
    // --------------------------------------------------

    cleanedTransactions.unpersist()
    persistedTransactions.unpersist()

    sc.stop()
  }
}
