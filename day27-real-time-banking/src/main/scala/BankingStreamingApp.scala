import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object BankingStreamingApp {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. Spark configuration
    // ---------------------------------------------------------
    val conf = new SparkConf()
      .setAppName("Day27-Real-Time-Banking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Checkpoint is required for window/stateful operations
    ssc.checkpoint("checkpoint")

    // ---------------------------------------------------------
    // 2. Read transaction stream
    // ---------------------------------------------------------
    val transactionStream = ssc.socketTextStream(
      "localhost",
      9999
    )

    // ---------------------------------------------------------
    // 3. Parse transaction events
    //
    // Format:
    // accountId,transactionId,amount,branchId
    // ---------------------------------------------------------
    val transactions = transactionStream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 4) {

        try {
          val accountId = parts(0).trim
          val transactionId = parts(1).trim
          val amount = parts(2).trim.toDouble
          val branchId = parts(3).trim

          Some((accountId, transactionId, amount, branchId))

        } catch {
          case _: NumberFormatException => None
        }

      } else {
        None
      }
    }

    // ---------------------------------------------------------
    // 4. Cache / persist transaction data
    // ---------------------------------------------------------
    val persistedTransactions =
      transactions.persist(StorageLevel.MEMORY_ONLY)

    // ---------------------------------------------------------
    // 5. Aggregate transactions by account
    // ---------------------------------------------------------
    val accountTotals = persistedTransactions
      .map {
        case (accountId, _, amount, _) =>
          (accountId, amount)
      }
      .reduceByKey(_ + _)

    accountTotals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== ACCOUNT TRANSACTION TOTALS ==========")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (accountId, total) =>
              println(
                f"Account: $accountId%-5s Total Amount: $total%.2f"
              )
          }

        println("================================================\n")
      }
    }

    // ---------------------------------------------------------
    // 6. Detect suspicious bursts
    //
    // Window:
    //   20 seconds
    //
    // Sliding interval:
    //   5 seconds
    //
    // If an account performs 3 or more transactions
    // within the window, flag it.
    // ---------------------------------------------------------
    val transactionCounts = persistedTransactions
      .map {
        case (accountId, _, _, _) =>
          (accountId, 1)
      }

    val burstDetection = transactionCounts
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(5)
      )
      .filter {
        case (_, count) =>
          count >= 3
      }

    burstDetection.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n******** SUSPICIOUS TRANSACTION BURST ********")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (accountId, count) =>
              println(
                s"ALERT -> Account $accountId has $count transactions in the last 20 seconds"
              )
          }

        println("***********************************************\n")
      }
    }

    // ---------------------------------------------------------
    // 7. Load small branch/risk reference data
    // ---------------------------------------------------------
    val branchRiskData = ssc.sparkContext
      .textFile("data/branch_risk.txt")
      .flatMap { line =>

        val parts = line.split(",")

        if (parts.length == 3) {
          Some(
            (
              parts(0).trim,
              (parts(1).trim, parts(2).trim)
            )
          )
        } else {
          None
        }
      }
      .collectAsMap()

    // ---------------------------------------------------------
    // 8. Broadcast branch/risk reference data
    // ---------------------------------------------------------
    val branchRiskBroadcast =
      ssc.sparkContext.broadcast(branchRiskData)

    // ---------------------------------------------------------
    // 9. Join transactions with branch/risk data
    // ---------------------------------------------------------
    val enrichedTransactions = persistedTransactions.map {

      case (accountId, transactionId, amount, branchId) =>

        val branchInfo =
          branchRiskBroadcast.value.getOrElse(
            branchId,
            ("Unknown", "Unknown")
          )

        (
          accountId,
          transactionId,
          amount,
          branchId,
          branchInfo._1,
          branchInfo._2
        )
    }

    enrichedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== ENRICHED TRANSACTIONS ==========")

        rdd.collect().foreach {
          case (
                accountId,
                transactionId,
                amount,
                branchId,
                branchName,
                riskLevel
              ) =>

            println(
              f"Account: $accountId%-5s " +
              f"Transaction: $transactionId%-8s " +
              f"Amount: $amount%-8.2f " +
              f"Branch: $branchName%-12s " +
              f"Risk: $riskLevel"
            )
        }

        println("============================================\n")
      }
    }

    // ---------------------------------------------------------
// 10. Partitioning demonstration
// ---------------------------------------------------------
persistedTransactions.foreachRDD { rdd =>

  if (!rdd.isEmpty()) {

    val partitionedTransactions =
      rdd
        .map {
          case (accountId, transactionId, amount, branchId) =>
            (accountId, (transactionId, amount, branchId))
        }
        .partitionBy(
          new org.apache.spark.HashPartitioner(4)
        )

    println(
      s"Number of partitions: ${partitionedTransactions.getNumPartitions}"
    )
  }
}

    // ---------------------------------------------------------
    // 11. Start streaming
    // ---------------------------------------------------------
    println("==============================================")
    println(" Day 27 Real-Time Banking Streaming Started")
    println(" Listening on localhost:9999")
    println(" Batch Interval: 5 seconds")
    println(" Suspicious Window: 20 seconds")
    println(" Sliding Interval: 5 seconds")
    println("==============================================")

    ssc.start()
    ssc.awaitTermination()
  }
}
