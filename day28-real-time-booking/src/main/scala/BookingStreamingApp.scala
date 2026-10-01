import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class BookingEvent(
  bookingId: String,
  userId: String,
  resourceId: String,
  eventType: String,
  quantity: Int,
  amount: Double
)

case class ResourceInfo(
  name: String,
  resourceType: String,
  capacity: Int
)

object BookingStreamingApp {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. Spark configuration
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day28-Real-Time-Booking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful processing
    ssc.checkpoint("checkpoint")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ---------------------------------------------------------
    // 2. Load reference data
    // ---------------------------------------------------------

    val referenceData =
      scala.io.Source
        .fromFile("data/resource_reference.txt")
        .getLines()
        .filter(_.trim.nonEmpty)
        .map { line =>
          val parts = line.split(",")

          (
            parts(0),
            ResourceInfo(
              parts(1),
              parts(2),
              parts(3).toInt
            )
          )
        }
        .toMap

    // Broadcast small reference data
    val broadcastReference =
      ssc.sparkContext.broadcast(referenceData)

    // ---------------------------------------------------------
    // 3. Create socket stream
    // ---------------------------------------------------------

    val rawStream =
      ssc.socketTextStream(
        "localhost",
        9999
      )

    println()
    println("====================================================")
    println("Day 28 Real-Time Booking Streaming Started")
    println("Listening on localhost:9999")
    println("Batch Interval : 5 seconds")
    println("Window Size    : 20 seconds")
    println("Sliding Interval: 5 seconds")
    println("====================================================")
    println()

    // ---------------------------------------------------------
    // 4. Parse booking events
    // ---------------------------------------------------------

    val bookingStream = rawStream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 6) {

        try {

          Some(
            BookingEvent(
              parts(0),
              parts(1),
              parts(2),
              parts(3).toUpperCase,
              parts(4).toInt,
              parts(5).toDouble
            )
          )

        } catch {
          case _: Exception =>
            None
        }

      } else {
        None
      }
    }

    // ---------------------------------------------------------
    // 5. Count invalid events
    // ---------------------------------------------------------

    val invalidEventAccumulator =
      ssc.sparkContext.longAccumulator(
        "Invalid Booking Events"
      )

    // ---------------------------------------------------------
    // 6. Pair RDD
    // ---------------------------------------------------------

    val resourceQuantityStream =
      bookingStream.map { event =>

        val quantity =
          if (event.eventType == "BOOK")
            event.quantity
          else
            -event.quantity

        (
          event.resourceId,
          quantity
        )
      }

    // ---------------------------------------------------------
    // 7. Stateful processing
    // ---------------------------------------------------------

    val updateState =
      (values: Seq[Int], state: Option[Int]) => {

        val previous =
          state.getOrElse(0)

        val current =
          previous + values.sum

        Some(current)
      }

    val bookingState =
      resourceQuantityStream
        .updateStateByKey[Int](updateState)

    // ---------------------------------------------------------
    // 8. Window operation
    // ---------------------------------------------------------

    val windowedBookings =
      bookingStream
        .map(event => (event.resourceId, event.quantity))
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(20),
          Seconds(5)
        )

    // ---------------------------------------------------------
    // 9. Process current booking events
    // ---------------------------------------------------------

    bookingStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== NEW BOOKING EVENTS ==========")

        rdd.collect().foreach { event =>

          val reference =
            broadcastReference.value.get(event.resourceId)

          reference match {

            case Some(info) =>

              println(
                f"Booking=${event.bookingId}%-5s " +
                f"Resource=${event.resourceId}%-4s " +
                f"Type=${info.resourceType}%-6s " +
                f"Event=${event.eventType}%-6s " +
                f"Qty=${event.quantity}%-3d " +
                f"Amount=${event.amount}%.2f"
              )

            case None =>

              invalidEventAccumulator.add(1)

              println(
                s"Invalid resource: ${event.resourceId}"
              )
          }
        }

        println(
          s"Invalid Events = ${invalidEventAccumulator.value}"
        )
      }
    }

    // ---------------------------------------------------------
    // 10. Display stateful occupancy
    // ---------------------------------------------------------

    bookingState.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== CURRENT OCCUPANCY ==========")

        rdd.collect().foreach {

          case (resourceId, bookedQuantity) =>

            broadcastReference.value.get(resourceId) match {

              case Some(info) =>

                val available =
                  info.capacity - bookedQuantity

                println(
                  f"$resourceId%-5s " +
                  f"${info.name}%-20s " +
                  f"Capacity=${info.capacity}%-4d " +
                  f"Booked=$bookedQuantity%-4d " +
                  f"Available=$available%-4d"
                )

              case None =>
            }
        }
      }
    }

    // ---------------------------------------------------------
    // 11. Display window statistics
    // ---------------------------------------------------------

    windowedBookings.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println(
          "========== BOOKINGS IN LAST 20 SECONDS =========="
        )

        rdd.collect().foreach {

          case (resourceId, quantity) =>

            val info =
              broadcastReference.value.get(resourceId)

            info match {

              case Some(resource) =>

                println(
                  f"$resourceId%-5s " +
                  f"${resource.name}%-20s " +
                  f"Quantity=$quantity"
                )

              case None =>
            }
        }
      }
    }

    // ---------------------------------------------------------
    // 12. Spark SQL reporting
    // ---------------------------------------------------------

    bookingStream.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val rows =
          rdd.collect().map { event =>

            val resource =
              broadcastReference.value.get(event.resourceId)

            val resourceName =
              resource
                .map(_.name)
                .getOrElse("Unknown")

            val resourceType =
              resource
                .map(_.resourceType)
                .getOrElse("Unknown")

            (
              event.bookingId,
              event.userId,
              event.resourceId,
              resourceName,
              resourceType,
              event.eventType,
              event.quantity,
              event.amount
            )
          }

        import spark.implicits._

        val reportDF =
          rows.toSeq.toDF(
            "booking_id",
            "user_id",
            "resource_id",
            "resource_name",
            "resource_type",
            "event_type",
            "quantity",
            "amount"
          )

        println()
        println("========== SPARK SQL REPORT ==========")

        reportDF.createOrReplaceTempView(
          "booking_report"
        )

        val summary =
          spark.sql(
            """
              SELECT
                resource_type,
                event_type,
                SUM(quantity) AS total_quantity,
                SUM(amount) AS total_amount
              FROM booking_report
              GROUP BY resource_type, event_type
              ORDER BY resource_type, event_type
            """
          )

        summary.show(false)
      }
    }

    // ---------------------------------------------------------
    // 13. Start streaming
    // ---------------------------------------------------------

    ssc.start()

    ssc.awaitTermination()
  }
}
