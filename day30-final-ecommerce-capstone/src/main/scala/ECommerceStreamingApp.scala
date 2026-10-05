import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ECommerceStreamingApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day30-ECommerce-Streaming")
      .setMaster("local[4]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpoint/day30-ecommerce")

    println("==============================================")
    println("DAY 30 - E-COMMERCE STREAMING")
    println("==============================================")
    println("Batch Interval : 5 seconds")
    println("Window         : 20 seconds")
    println("Sliding        : 5 seconds")
    println("Waiting for live e-commerce events...")
    println("==============================================")

    val stream = ssc.socketTextStream("localhost", 9999)

    val transactions = stream.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 5) {
        try {
          Some((
            parts(0),
            parts(1),
            parts(2),
            parts(3).toInt,
            parts(4).toDouble
          ))
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    val validTransactions = transactions.filter {
      case (_, _, _, quantity, amount) =>
        quantity > 0 && amount > 0
    }

    val customerRevenue = validTransactions
      .map {
        case (_, customerId, _, _, amount) =>
          (customerId, amount)
      }
      .reduceByKey(_ + _)

    val windowRevenue = validTransactions
      .map {
        case (_, _, _, _, amount) =>
          ("E-Commerce", amount)
      }
      .reduceByKeyAndWindow(
        (a: Double, b: Double) => a + b,
        Seconds(20),
        Seconds(5)
      )

    customerRevenue.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n--- Customer Revenue ---")
        rdd.collect().foreach {
          case (customer, revenue) =>
            println(f"$customer -> $revenue%.2f")
        }
      }
    }

    windowRevenue.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n--- 20-Second Window Revenue ---")
        rdd.collect().foreach {
          case (_, revenue) =>
            println(f"Total Revenue -> $revenue%.2f")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
