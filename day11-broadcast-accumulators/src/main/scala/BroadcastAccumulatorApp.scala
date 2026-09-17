import org.apache.spark.{SparkConf, SparkContext}

object BroadcastAccumulatorApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Broadcast and Accumulators")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("===== DAY 11: BROADCAST AND ACCUMULATORS =====")

    // --------------------------------------------------
    // 1. LOAD PRODUCT MASTER TABLE
    // --------------------------------------------------

    val productMaster = sc.textFile("data/product_master.txt")

    val productMap = productMaster.map { line =>
      val parts = line.split(",")

      val productId = parts(0)
      val productName = parts(1)
      val category = parts(2)
      val price = parts(3).toDouble

      (productId, (productName, category, price))
    }.collect().toMap

    println("\n===== PRODUCT MASTER MAP =====")

    productMap.toSeq
      .sortBy(_._1)
      .foreach {
        case (productId, (name, category, price)) =>
          println(
            productId + " -> " +
              name + ", " +
              category + ", " +
              price
          )
      }

    // --------------------------------------------------
    // 2. BROADCAST MASTER MAP
    // --------------------------------------------------

    val broadcastProductMap =
      sc.broadcast(productMap)

    println("\n===== BROADCAST VARIABLE =====")
    println("Product master map has been broadcast to executors.")
    println("Broadcast entries: " +
      broadcastProductMap.value.size)

    // --------------------------------------------------
    // 3. ACCUMULATOR FOR BAD RECORDS
    // --------------------------------------------------

    val badRecords =
      sc.longAccumulator("Bad Transaction Records")

    // --------------------------------------------------
    // 4. LOAD TRANSACTIONS
    // --------------------------------------------------

    val transactions =
      sc.textFile("data/transactions.txt")

    println("\n===== TRANSACTION VALIDATION =====")

    val validationResults = transactions.map { line =>

      val parts = line.split(",")

      val transactionId = parts(0)
      val productId = parts(1)
      val transactionPrice = parts(2).toDouble

      val masterProduct =
        broadcastProductMap.value.get(productId)

      masterProduct match {

        case Some((productName, category, masterPrice)) =>

          if (transactionPrice == masterPrice) {

            transactionId +
              " -> VALID | " +
              productName +
              " | " +
              category +
              " | Price: " +
              transactionPrice

          } else {

            badRecords.add(1)

            transactionId +
              " -> INVALID PRICE | " +
              productName +
              " | Expected: " +
              masterPrice +
              " | Received: " +
              transactionPrice
          }

        case None =>

          badRecords.add(1)

          transactionId +
            " -> INVALID PRODUCT | ProductID: " +
            productId
      }
    }

    // --------------------------------------------------
    // 5. TRIGGER EXECUTION
    // --------------------------------------------------

    val results = validationResults.collect()

    results.foreach(println)

    // --------------------------------------------------
    // 6. ACCUMULATOR RESULT
    // --------------------------------------------------

    println("\n===== ACCUMULATOR RESULT =====")
    println("Total bad records: " + badRecords.value)

    // --------------------------------------------------
    // 7. VALID / INVALID SUMMARY
    // --------------------------------------------------

    val validCount =
      results.count(_.contains("VALID |"))

    val invalidCount =
      results.count(_.contains("INVALID"))

    println("\n===== VALIDATION SUMMARY =====")
    println("Total transactions: " + results.length)
    println("Valid transactions: " + validCount)
    println("Invalid transactions: " + invalidCount)

    // --------------------------------------------------
    // 8. DRIVER VARIABLE EXPLANATION
    // --------------------------------------------------

    println("\n===== DRIVER VARIABLE VS ACCUMULATOR =====")

    println(
      "A normal driver variable should not be used " +
        "for distributed updates."
    )

    println(
      "Executors run tasks independently, so updates " +
        "to a normal driver variable are not reliably " +
        "shared back with the driver."
    )

    println(
      "An accumulator is designed for distributed " +
        "counter-style updates."
    )

    println(
      "The driver reads the accumulator value after " +
        "the Spark action completes."
    )

    // --------------------------------------------------
    // 9. BROADCAST + RDD PROCESSING FLOW
    // --------------------------------------------------

    println("\n===== BROADCAST + RDD PROCESSING FLOW =====")

    println("1. Load small product master table.")
    println("2. Convert master table into a Map.")
    println("3. Broadcast the Map to executors.")
    println("4. Read transaction RDD.")
    println("5. Validate each transaction using broadcast data.")
    println("6. Increment accumulator for bad records.")
    println("7. Execute the RDD using collect().")
    println("8. Read the accumulator result on the driver.")

    // --------------------------------------------------
    // 10. CLEANUP
    // --------------------------------------------------

    broadcastProductMap.destroy()

    sc.stop()
  }
}
