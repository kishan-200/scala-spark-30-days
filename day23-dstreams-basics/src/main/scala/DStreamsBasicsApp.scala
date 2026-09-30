import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsBasicsApp {

  def main(args: Array[String]): Unit = {

    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName("Day23-DStreams-Basics")
      .setMaster("local[*]")

    // Create StreamingContext with 5-second batch interval
    val ssc = new StreamingContext(conf, Seconds(5))

    // Read streaming data from socket
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each incoming line into words
    val words = lines.flatMap(_.split("\\s+"))

    // Keep only ERROR messages
    val errorWords = words.filter(_.toUpperCase == "ERROR")

    // Convert each ERROR into (ERROR, 1)
    val errorPairs = errorWords.map(word => (word.toUpperCase, 1))

    // Count ERROR messages
    val errorCounts = errorPairs.reduceByKey(_ + _)

    // Print results for every micro-batch
    errorCounts.print()

    // Start streaming
    ssc.start()

    // Wait for the streaming application to terminate
    ssc.awaitTermination()
  }
}
