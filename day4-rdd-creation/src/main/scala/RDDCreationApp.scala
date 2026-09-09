import org.apache.spark.{SparkConf, SparkContext}

object RDDCreationApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("RDD Creation Application")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // 1. Create RDD from a Scala collection
    val numbersRDD = sc.parallelize(1 to 10)

    println("===== RDD FROM COLLECTION =====")
    println("Numbers: " + numbersRDD.collect().mkString(", "))

    // 2. Create RDD from a text file
    val transactionsRDD =
      sc.textFile("data/transactions.txt")

    println("\n===== RDD FROM TEXT FILE =====")
    transactionsRDD.collect().foreach(println)

    // 3. map
    val transactionValues = transactionsRDD.map { line =>
      line.split(",")(2).toDouble
    }

    println("\n===== MAP =====")
    println("Sales values: " +
      transactionValues.collect().mkString(", "))

    // 4. filter
    val highValueTransactions =
      transactionValues.filter(_ >= 10000)

    println("\n===== FILTER =====")
    println("Sales >= 10000: " +
      highValueTransactions.collect().mkString(", "))

    // 5. flatMap
    val wordsRDD = transactionsRDD.flatMap(_.split(","))

    println("\n===== FLATMAP =====")
    println("Fields: " +
      wordsRDD.collect().mkString(", "))

    // 6. Calculate total sales
    val totalSales = transactionValues.reduce(_ + _)

    println("\n===== TOTAL SALES =====")
    println("Total Sales: " + totalSales)

    // 7. Inspect partitions
    println("\n===== PARTITION INFORMATION =====")
    println("Number of partitions: " +
      transactionsRDD.getNumPartitions)

    println("Default parallelism: " +
      sc.defaultParallelism)

    sc.stop()
  }
}
