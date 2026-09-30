import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object StatelessStatefulApp {

  def main(args: Array[String]): Unit = {

    // =========================================================
    // 1. CREATE SPARK CONFIGURATION
    // =========================================================

    val conf = new SparkConf()
      .setAppName("Day24-Stateless-Stateful")
      .setMaster("local[*]")

    // =========================================================
    // 2. CREATE STREAMING CONTEXT
    // =========================================================

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Required for stateful transformations
    ssc.checkpoint("checkpoint")

    // =========================================================
    // 3. READ BANK TRANSACTIONS
    // =========================================================

    val lines = ssc.socketTextStream("localhost", 9999)

    // Expected input:
    // account_id,transaction_amount

    val transactions = lines
      .filter(_.trim.nonEmpty)
      .map { line =>
        val parts = line.split(",")

        val accountId = parts(0).trim
        val amount = parts(1).trim.toDouble

        (accountId, amount)
      }

    // =========================================================
    // 4. STATELESS PROCESSING
    // =========================================================

    val currentBatchCounts = transactions
      .map { case (accountId, amount) =>
        (accountId, 1)
      }
      .reduceByKey(_ + _)

    println("\n=== STATELESS CURRENT-BATCH COUNTS ===")

    currentBatchCounts.foreachRDD { rdd =>
  println("\n===== STATELESS CURRENT BATCH =====")
  rdd.collect().foreach(println)
}

    // =========================================================
    // 5. STATEFUL PROCESSING
    // =========================================================

    val runningCounts = transactions
  .map { case (accountId, amount) =>
    (accountId, 1)
  }
  .updateStateByKey[Int] {
    (newValues: Seq[Int], previousState: Option[Int]) =>

      val newCount = newValues.sum
      val oldCount = previousState.getOrElse(0)

      Some(oldCount + newCount)
  }

    println("\n=== STATEFUL RUNNING COUNTS ===")

    runningCounts.foreachRDD { rdd =>
  println("\n===== STATEFUL RUNNING COUNT =====")
  rdd.collect().foreach(println)
}

    // =========================================================
    // 6. START STREAMING
    // =========================================================

    ssc.start()

    ssc.awaitTermination()
  }
}
