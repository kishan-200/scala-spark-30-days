object CustomerOrderDemo {

  def main(args: Array[String]): Unit = {

    // Customer records
    val customers = List(
      ("C001", "Rahul"),
      ("C002", "Kishan"),
      ("C003", "Anil"),
      ("C004", "Priya")
    )

    // Order records
    val orders = List(
      ("O101", "C001", "Laptop", 1, 60000.0),
      ("O102", "C002", "Mouse", 2, 800.0),
      ("O103", "C001", "Keyboard", 1, 1500.0),
      ("O104", "C003", "Monitor", 2, 12000.0),
      ("O105", "C004", "Headphones", 3, 2500.0)
    )

    println("===== CUSTOMER ORDERS =====")

    // Combine customers and their orders
    val customerOrders = for {
      customer <- customers
      order <- orders
      if customer._1 == order._2
    } yield {
      val orderId = order._1
      val product = order._3
      val quantity = order._4
      val price = order._5
      val total = quantity * price

      (customer._2, orderId, product, quantity, total)
    }

    customerOrders.foreach {
      case (customer, orderId, product, quantity, total) =>
        println(
          customer +
          " | Order: " + orderId +
          " | Product: " + product +
          " | Quantity: " + quantity +
          " | Total: ₹" + total
        )
    }

    println("\n===== TOTAL ORDERS =====")
    println(customerOrders.size)
  }
}
