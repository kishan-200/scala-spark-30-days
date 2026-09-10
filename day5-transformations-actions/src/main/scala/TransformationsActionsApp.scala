import org.apache.spark.{SparkConf, SparkContext}

object TransformationsActionsApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Transformations and Actions")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    // Read application logs
    val logs = sc.textFile("data/application.log")

    println("===== ORIGINAL LOGS =====")
    logs.collect().foreach(println)

    // ==============================
    // TRANSFORMATIONS
    // ==============================

    // 1. map
    val logLengths = logs.map(_.length)

    println("\n===== MAP =====")
    println("Log line lengths: " +
      logLengths.collect().mkString(", "))

    // 2. filter
    val errorLogs = logs.filter(_.contains("ERROR"))

    println("\n===== FILTER =====")
    errorLogs.collect().foreach(println)

    // 3. flatMap
    val words = logs.flatMap(_.split(" "))

    println("\n===== FLATMAP =====")
    println("First 10 words: " +
      words.take(10).mkString(", "))

    // 4. distinct
    val logLevels = logs
      .flatMap(_.split(" "))
      .filter(word =>
        word == "INFO" ||
        word == "WARN" ||
        word == "ERROR"
      )
      .distinct()

    println("\n===== DISTINCT =====")
    println("Log levels: " +
      logLevels.collect().mkString(", "))

    // 5. union
    val infoLogs = logs.filter(_.contains("INFO"))
    val warnLogs = logs.filter(_.contains("WARN"))

    val infoAndWarnLogs = infoLogs.union(warnLogs)

    println("\n===== UNION =====")
    println("INFO + WARN count: " +
      infoAndWarnLogs.count())

    // ==============================
    // ACTIONS
    // ==============================

    println("\n===== ACTIONS =====")

    // count
    println("Total log lines: " + logs.count())

    // collect
    println("All log levels: " +
      logLevels.collect().mkString(", "))

    // first
    println("First log: " + logs.first())

    // take
    println("First 3 logs:")
    logs.take(3).foreach(println)

    // reduce
    val totalCharacters =
      logLengths.reduce(_ + _)

    println("Total characters: " +
      totalCharacters)

    // ==============================
    // ERROR ANALYZER
    // ==============================

    println("\n===== ERROR LOG ANALYZER =====")

    val errorCount = logs
      .filter(_.contains("ERROR"))
      .count()

    println("Total ERROR messages: " +
      errorCount)

    sc.stop()
  }
}
