import org.apache.spark.{SparkConf, SparkContext}

object CustomerPartitionApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Customer Partition Application")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    // Read customer file with 4 partitions
    val customersRDD =
      sc.textFile("data/customers.txt", 4)

    println("===== CUSTOMER FILE =====")
    println("Number of customers: " +
      customersRDD.count())

    println("\n===== PARTITION INFORMATION =====")
    println("Number of partitions: " +
      customersRDD.getNumPartitions)

    println("Default parallelism: " +
      sc.defaultParallelism)

    println("\n===== CUSTOMERS BY PARTITION =====")

    val partitionData = customersRDD.mapPartitionsWithIndex {
      (partitionIndex, records) =>

        val customers = records.toList

        Iterator(
          "Partition " + partitionIndex +
          " → " + customers.size +
          " customers"
        )
    }

    partitionData.collect().foreach(println)

    sc.stop()
  }
}
