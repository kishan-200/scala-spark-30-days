import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowOperationsApp {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. Spark configuration
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[*]")

    // ---------------------------------------------------------
    // 2. Create StreamingContext
    // ---------------------------------------------------------

    // Spark creates one micro-batch every 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))
   

ssc.checkpoint("checkpoint")

    ssc.sparkContext.setLogLevel("ERROR")

    // ---------------------------------------------------------
    // 3. Read transaction stream
    // ---------------------------------------------------------

    val lines = ssc.socketTextStream("localhost", 9999)

    // Input format:
    // account_id,amount
    //
    // Example:
    // A101,500
    // A102,700

    val transactions = lines
      .filter(_.trim.nonEmpty)
      .map { line =>
        val parts = line.split(",")

        val account = parts(0)
        val amount = parts(1).toDouble

        (account, amount)
      }

    // ---------------------------------------------------------
    // 4. countByWindow
    // ---------------------------------------------------------

    // Window = 30 seconds
    // Slide = 10 seconds
    //
    // Counts all transactions appearing in the
    // most recent 30-second window.

    val transactionCount = transactions
      .map(_ => 1L)
      .countByWindow(
        Seconds(30),
        Seconds(10)
      )

    transactionCount.foreachRDD { rdd =>

      println()
      println("===== TRANSACTION COUNT IN LAST 30 SECONDS =====")

      rdd.collect().foreach(println)
    }

    // ---------------------------------------------------------
    // 5. reduceByKeyAndWindow
    // ---------------------------------------------------------

    // Calculate rolling transaction amount
    // for each account during the latest 30 seconds.

    val rollingSales = transactions
  .reduceByKeyAndWindow(
    (a: Double, b: Double) => a + b,
    Seconds(30),
    Seconds(10)
  )

    rollingSales.foreachRDD { rdd =>

      println()
      println("===== ROLLING SALES TOTAL =====")

      rdd.collect().foreach {
        case (account, amount) =>
          println(
            f"$account -> $amount%.2f"
          )
      }
    }

    // ---------------------------------------------------------
    // 6. Detect sudden increase
    // ---------------------------------------------------------

    // If the total transaction amount in the
    // 30-second window exceeds 2000,
    // report high transaction activity.

    rollingSales.foreachRDD { rdd =>

      val highActivity = rdd.filter {
        case (_, amount) => amount > 2000
      }

      if (!highActivity.isEmpty()) {

        println()
        println("===== HIGH TRANSACTION ACTIVITY =====")

        highActivity.collect().foreach {
          case (account, amount) =>
            println(
              f"ALERT: $account has rolling transactions of $amount%.2f"
            )
        }
      }
    }

    // ---------------------------------------------------------
    // 7. Start streaming
    // ---------------------------------------------------------

    ssc.start()

    println()
    println("Day 25 Window Operations Streaming Started")
    println("Batch Interval : 5 seconds")
    println("Window Size    : 30 seconds")
    println("Slide Interval : 10 seconds")
    println("Listening on   : localhost:9999")

    ssc.awaitTermination()
  }
}
