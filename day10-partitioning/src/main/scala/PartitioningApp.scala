import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitioningApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark Partitioning")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 10: PARTITIONING =====")

    // --------------------------------------------------
    // 1. CREATE RDD WITH TOO FEW PARTITIONS
    // --------------------------------------------------

    val sales = sc.textFile("data/sales.txt", 1)

    println("\n===== ORIGINAL RDD =====")
    println("Number of records: " + sales.count())
    println("Original partitions: " + sales.getNumPartitions)

    // --------------------------------------------------
    // 2. REPARTITION
    // --------------------------------------------------

    val repartitionedSales = sales.repartition(4)

    println("\n===== REPARTITION =====")
    println("Partitions after repartition(4): " +
      repartitionedSales.getNumPartitions)

    // --------------------------------------------------
    // 3. COALESCE
    // --------------------------------------------------

    val coalescedSales = repartitionedSales.coalesce(2)

    println("\n===== COALESCE =====")
    println("Partitions after coalesce(2): " +
      coalescedSales.getNumPartitions)

    // --------------------------------------------------
    // 4. INCREASING / DECREASING PARTITIONS
    // --------------------------------------------------

    println("\n===== PARTITIONING GUIDELINES =====")

    println("Increasing partitions:")
    println("- Helps distribute large workloads across more tasks.")
    println("- Can improve parallelism when partitions are too large.")
    println("- repartition() can be used to increase partitions.")

    println("\nDecreasing partitions:")
    println("- Reduces the number of tasks.")
    println("- Can reduce scheduling overhead for smaller workloads.")
    println("- coalesce() can reduce partitions with less movement.")

    // --------------------------------------------------
    // 5. CREATE PAIR RDD
    // --------------------------------------------------

    val salesByDepartment = sales.map { line =>
      val parts = line.split(",")
      (parts(1), parts(2).toDouble)
    }

    println("\n===== PAIR RDD =====")
    println("Pair RDD partitions: " +
      salesByDepartment.getNumPartitions)

    // --------------------------------------------------
    // 6. partitionBy
    // --------------------------------------------------

    val partitionedByDepartment =
      salesByDepartment.partitionBy(new HashPartitioner(4))

    println("\n===== partitionBy =====")
    println("Partitions after partitionBy(HashPartitioner(4)): " +
      partitionedByDepartment.getNumPartitions)

    // --------------------------------------------------
    // 7. INSPECT PARTITION CONTENT
    // --------------------------------------------------

    println("\n===== PARTITION CONTENT =====")

    partitionedByDepartment
      .mapPartitionsWithIndex {
        case (partitionId, records) =>
          Iterator(
            "Partition " + partitionId +
            " -> " + records.toList.mkString(", ")
          )
      }
      .collect()
      .foreach(println)

    // --------------------------------------------------
    // 8. AGGREGATION AFTER PARTITIONING
    // --------------------------------------------------

    val revenueByDepartment =
      partitionedByDepartment.reduceByKey(_ + _)

    println("\n===== REVENUE BY DEPARTMENT =====")

    revenueByDepartment
      .collect()
      .sortBy(_._1)
      .foreach {
        case (department, revenue) =>
          println(department + " -> " + revenue)
      }

    // --------------------------------------------------
    // 9. OPTIMIZATION SCENARIO
    // --------------------------------------------------

    println("\n===== TOO FEW PARTITIONS: OPTIMIZATION =====")

    println("Problem:")
    println("- Original sales RDD has only 1 partition.")
    println("- A large dataset with very few partitions")
    println("  limits parallel processing.")

    println("\nOptimization:")
    println("- Increase partitions using repartition(4).")
    println("- This allows the workload to be distributed")
    println("  across more tasks.")

    println("\nBefore optimization: " +
      sales.getNumPartitions + " partition")

    println("After optimization: " +
      repartitionedSales.getNumPartitions + " partitions")

    // --------------------------------------------------
    // 10. FINAL SUMMARY
    // --------------------------------------------------

    println("\n===== DAY 10 SUMMARY =====")
    println("Original partitions      : " + sales.getNumPartitions)
    println("After repartition(4)     : " +
      repartitionedSales.getNumPartitions)
    println("After coalesce(2)        : " +
      coalescedSales.getNumPartitions)
    println("After partitionBy(4)     : " +
      partitionedByDepartment.getNumPartitions)

    sc.stop()
  }
}
