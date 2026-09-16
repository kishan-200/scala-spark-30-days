import org.apache.spark.{SparkConf, SparkContext}

object DAGExecutionApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("DAG and Spark Execution")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 8: DAG AND SPARK EXECUTION =====")

    // Read input data
    val sales = sc.textFile("data/sales.txt", 4)

    println("\n===== INPUT DATA =====")
    sales.collect().foreach(println)

    println("\n===== INPUT PARTITIONS =====")
    println("Number of partitions: " +
      sales.getNumPartitions)

    // Narrow transformation: map
    val salesPairs = sales.map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toDouble)
    }

    // Narrow transformation: filter
    val highValueSales = salesPairs.filter {
      case (_, amount) => amount >= 1000
    }

    // Wide transformation: reduceByKey
    // This operation introduces a shuffle.
    val totalSalesByProduct =
      highValueSales.reduceByKey(_ + _)

    // Narrow transformation after shuffle
    val finalResult =
      totalSalesByProduct.map {
        case (product, total) =>
          product + " -> " + total
      }

    println("\n===== FINAL RESULT =====")
    finalResult.collect().foreach(println)

    println("\n===== SECOND ACTION: COUNT =====")
    println("Number of products: " +
      finalResult.count())

    println("\n===== RDD LINEAGE / DAG =====")
    println(finalResult.toDebugString)

    println("\n===== PARTITION INFORMATION =====")
    println("Input partitions: " +
      sales.getNumPartitions)

    println("Pair RDD partitions: " +
      salesPairs.getNumPartitions)

    println("After reduceByKey partitions: " +
      totalSalesByProduct.getNumPartitions)

    println("Final RDD partitions: " +
      finalResult.getNumPartitions)

    println("\n===== EXECUTION CONCEPT =====")
    println("Job = complete computation triggered by an action.")
    println("Stage = group of tasks separated by shuffle boundaries.")
    println("Task = unit of work for one partition.")
    println("Partition = portion of distributed dataset.")

    println("\n===== TRANSFORMATION TYPES =====")
    println("map        -> Narrow transformation")
    println("filter     -> Narrow transformation")
    println("reduceByKey -> Wide transformation + Shuffle")
    println("map        -> Narrow transformation")

    println("\n===== STAGE PREDICTION =====")
    println("reduceByKey introduces a shuffle boundary.")
    println("Therefore, this pipeline is divided into stages around")
    println("the reduceByKey shuffle boundary.")

    sc.stop()
  }
}
