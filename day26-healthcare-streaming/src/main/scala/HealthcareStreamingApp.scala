import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object HealthcareStreamingApp {

  case class PatientVital(
      patientId: String,
      heartRate: Double,
      spo2: Double,
      temperature: Double,
      systolicBP: Double,
      diastolicBP: Double
  )

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. Spark configuration
    // ---------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day26-Healthcare-Streaming")
      .setMaster("local[*]")

    // ---------------------------------------------------------
    // 2. Streaming context
    // ---------------------------------------------------------

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful processing
    ssc.checkpoint("checkpoint")

    // ---------------------------------------------------------
    // 3. Broadcast normal vital thresholds
    // ---------------------------------------------------------

    val thresholds = Map(
      "heartRateMin" -> 60.0,
      "heartRateMax" -> 100.0,
      "spo2Min" -> 95.0,
      "temperatureMin" -> 36.0,
      "temperatureMax" -> 37.5,
      "systolicMin" -> 90.0,
      "systolicMax" -> 140.0,
      "diastolicMin" -> 60.0,
      "diastolicMax" -> 90.0
    )

    val broadcastThresholds = ssc.sparkContext.broadcast(thresholds)

    // ---------------------------------------------------------
    // 4. Accumulator for abnormal readings
    // ---------------------------------------------------------

    val abnormalCounter =
      ssc.sparkContext.longAccumulator("Abnormal Vital Count")

    // ---------------------------------------------------------
    // 5. Read socket stream
    // ---------------------------------------------------------

    val lines = ssc.socketTextStream("localhost", 9999)

    // ---------------------------------------------------------
    // 6. Validate and parse input
    // ---------------------------------------------------------

    val vitals: DStream[PatientVital] = lines
      .filter(_.trim.nonEmpty)
      .flatMap { line =>

        val parts = line.split(",")

        if (parts.length == 6) {

          try {

            Some(
              PatientVital(
                parts(0).trim,
                parts(1).trim.toDouble,
                parts(2).trim.toDouble,
                parts(3).trim.toDouble,
                parts(4).trim.toDouble,
                parts(5).trim.toDouble
              )
            )

          } catch {
            case _: NumberFormatException =>
              None
          }

        } else {
          None
        }
      }

    // ---------------------------------------------------------
    // 7. Detect abnormal vitals
    // ---------------------------------------------------------

    val abnormalVitals = vitals.map { vital =>

      val t = broadcastThresholds.value

      val abnormalReasons = scala.collection.mutable.ListBuffer[String]()

      if (
        vital.heartRate < t("heartRateMin") ||
        vital.heartRate > t("heartRateMax")
      ) {
        abnormalReasons += "Heart Rate"
      }

      if (vital.spo2 < t("spo2Min")) {
        abnormalReasons += "SpO2"
      }

      if (
        vital.temperature < t("temperatureMin") ||
        vital.temperature > t("temperatureMax")
      ) {
        abnormalReasons += "Temperature"
      }

      if (
        vital.systolicBP < t("systolicMin") ||
        vital.systolicBP > t("systolicMax")
      ) {
        abnormalReasons += "Systolic BP"
      }

      if (
        vital.diastolicBP < t("diastolicMin") ||
        vital.diastolicBP > t("diastolicMax")
      ) {
        abnormalReasons += "Diastolic BP"
      }

      (vital, abnormalReasons.toList)
    }.filter {
      case (_, reasons) => reasons.nonEmpty
    }

    // ---------------------------------------------------------
    // 8. Count abnormal readings using accumulator
    // ---------------------------------------------------------

    abnormalVitals.foreachRDD { rdd =>

      rdd.foreach { case (vital, reasons) =>

        abnormalCounter.add(1)

        println(
          s"ALERT | Patient=${vital.patientId} | " +
          s"HR=${vital.heartRate} | " +
          s"SpO2=${vital.spo2} | " +
          s"Temp=${vital.temperature} | " +
          s"BP=${vital.systolicBP}/${vital.diastolicBP} | " +
          s"Abnormal=${reasons.mkString(",")}"
        )
      }
    }

    // ---------------------------------------------------------
    // 9. Stateful processing
    // Track abnormal readings per patient
    // ---------------------------------------------------------

    val abnormalByPatient = abnormalVitals
      .map {
        case (vital, _) =>
          (vital.patientId, 1)
      }

    val runningAbnormalCounts = abnormalByPatient.updateStateByKey[Int] {
      (newValues: Seq[Int], previousState: Option[Int]) =>

        val currentCount = newValues.sum
        val previousCount = previousState.getOrElse(0)

        Some(previousCount + currentCount)
    }

    // ---------------------------------------------------------
    // 10. Print running abnormal count
    // ---------------------------------------------------------

    runningAbnormalCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== RUNNING ABNORMAL COUNTS ==========")

        rdd.collect().sortBy(_._1).foreach {
          case (patientId, count) =>
            println(
              s"Patient $patientId -> $count abnormal readings"
            )
        }

        println("============================================")
      }
    }

    // ---------------------------------------------------------
    // 11. Window processing
    // Detect repeated abnormal readings
    // ---------------------------------------------------------

    val abnormalWindow = abnormalByPatient
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(5)
      )

    abnormalWindow.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== 20 SECOND ABNORMAL WINDOW ==========")

        rdd.collect().sortBy(_._1).foreach {
          case (patientId, count) =>

            if (count >= 2) {
              println(
                s"REPEATED ALERT | Patient $patientId | " +
                s"$count abnormal readings in recent window"
              )
            } else {
              println(
                s"Window | Patient $patientId | " +
                s"$count abnormal reading"
              )
            }
        }

        println("===============================================")
      }
    }

    // ---------------------------------------------------------
    // 12. Start streaming
    // ---------------------------------------------------------

    println()
    println("==============================================")
    println("Healthcare Streaming Application Started")
    println("Listening on localhost:9999")
    println("Batch Interval: 5 seconds")
    println("Window: 20 seconds")
    println("==============================================")

    ssc.start()
    ssc.awaitTermination()
  }
}
