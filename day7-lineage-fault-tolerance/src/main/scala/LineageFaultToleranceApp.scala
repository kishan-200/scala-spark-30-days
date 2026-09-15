import org.apache.spark.{SparkConf, SparkContext}

object LineageFaultToleranceApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Lineage and Fault Tolerance")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 7: LINEAGE AND FAULT TOLERANCE =====")

    // Create RDD from input file
    val numbers = sc.textFile("data/numbers.txt", 4)

    println("\n===== ORIGINAL RDD =====")
    println("Number of partitions: " +
      numbers.getNumPartitions)

    println("Original numbers:")
    numbers.collect().foreach(println)

    // Step 1: Filter even numbers
    val evenNumbers = numbers
      .map(_.toInt)
      .filter(_ % 2 == 0)

    // Step 2: Square the numbers
    val squaredNumbers = evenNumbers
      .map(number => number * number)

    // Step 3: Keep numbers greater than 50
    val largeSquares = squaredNumbers
      .filter(_ > 50)

    println("\n===== TRANSFORMATION RESULT =====")
    println("Numbers after transformations:")
    largeSquares.collect().foreach(println)

    // Demonstrate immutability
    println("\n===== IMMUTABILITY =====")
    println("Original RDD remains unchanged:")
    numbers.collect().foreach(println)

    // Trigger an action
    val resultCount = largeSquares.count()

    println("\n===== ACTION =====")
    println("Count of final values: " + resultCount)

    // Display RDD lineage
    println("\n===== RDD LINEAGE =====")
    println(largeSquares.toDebugString)

    // Partition information
    println("\n===== PARTITION INFORMATION =====")
    println("Original RDD partitions: " +
      numbers.getNumPartitions)

    println("Final RDD partitions: " +
      largeSquares.getNumPartitions)

    println("\n===== FAULT TOLERANCE CONCEPT =====")
    println("RDDs are immutable and Spark records their lineage.")
    println("If a partition is lost, Spark can recompute")
    println("that partition using the lineage information.")
    println("No manual recovery of the lost partition is required.")

    sc.stop()
  }
}
