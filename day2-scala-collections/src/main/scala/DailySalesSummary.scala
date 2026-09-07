object DailySalesSummary {

  def main(args: Array[String]): Unit = {

    // Daily sales records:
    // Order ID, Product, Quantity, Price
    val sales = List(
      ("O101", "Laptop", 1, 60000.0),
      ("O102", "Mouse", 2, 800.0),
      ("O103", "Keyboard", 1, 1500.0),
      ("O104", "Monitor", 2, 12000.0),
      ("O105", "Headphones", 3, 2500.0),
      ("O106", "Laptop", 1, 60000.0)
    )

    println("===== DAILY SALES RECORDS =====")

    sales.foreach {
      case (orderId, product, quantity, price) =>
        val total = quantity * price

        println(
          orderId +
          " | " + product +
          " | Quantity: " + quantity +
          " | Price: ₹" + price +
          " | Total: ₹" + total
        )
    }

    // Calculate total value of every order
    val orderTotals = sales.map {
      case (_, _, quantity, price) =>
        quantity * price
    }

    // Total sales amount
    val totalSales = orderTotals.reduce((a, b) => a + b)

    // Total quantity sold
    val totalQuantity = sales
      .map {
        case (_, _, quantity, _) =>
          quantity
      }
      .reduce((a, b) => a + b)

    // Number of orders
    val totalOrders = sales.size

    // Average order value
    val averageOrderValue = totalSales / totalOrders

    // Highest-value order
    val highestOrderValue = orderTotals.reduce((a, b) => {
      if (a > b) a else b
    })

    // Product-wise total quantities
    val productQuantities = sales
      .map {
        case (_, product, quantity, _) =>
          product -> quantity
      }
      .groupBy {
        case (product, _) =>
          product
      }
      .map {
        case (product, values) =>
          product -> values.map(_._2).sum
      }

    println("\n===== DAILY SALES SUMMARY =====")

    println("Total Orders: " + totalOrders)
    println("Total Quantity Sold: " + totalQuantity)
    println("Total Sales: ₹" + totalSales)
    println("Average Order Value: ₹" + averageOrderValue)
    println("Highest Order Value: ₹" + highestOrderValue)

    println("\n===== PRODUCT-WISE QUANTITY =====")

    productQuantities.foreach {
      case (product, quantity) =>
        println(product + " -> " + quantity)
    }
  }
}
