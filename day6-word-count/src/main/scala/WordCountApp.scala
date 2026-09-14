import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Word Count Application")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    // Read the application log
    val lines = sc.textFile("data/application.log")

    println("===== CLASSIC WORD COUNT =====")

    // flatMap → map → reduceByKey
    val wordCounts = lines
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts.collect()
      .sortBy(-_._2)
      .foreach {
        case (word, count) =>
          println(word + " -> " + count)
      }

    // ==========================================
    // CASE-INSENSITIVE WORD COUNT
    // ==========================================

    println("\n===== CASE-INSENSITIVE WORD COUNT =====")

    val caseInsensitiveCounts = lines
      .flatMap(_.split("\\s+"))
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    caseInsensitiveCounts.collect()
      .sortBy(-_._2)
      .foreach {
        case (word, count) =>
          println(word + " -> " + count)
      }

    // ==========================================
    // CLEAN WORD COUNT
    // ==========================================

    println("\n===== CLEAN WORD COUNT =====")

    val cleanCounts = lines
      .flatMap(_.split("\\s+"))
      .map(_.toLowerCase.replaceAll("[^a-z0-9]", ""))
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    cleanCounts.collect()
      .sortBy(-_._2)
      .foreach {
        case (word, count) =>
          println(word + " -> " + count)
      }

    // ==========================================
    // TOP 10 MOST FREQUENT WORDS
    // ==========================================

    println("\n===== TOP 10 MOST FREQUENT WORDS =====")

    val top10Words = cleanCounts
      .sortBy({
        case (_, count) => -count
      })
      .take(10)

    top10Words.foreach {
      case (word, count) =>
        println(word + " -> " + count)
    }

    sc.stop()
  }
}
