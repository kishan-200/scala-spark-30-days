object ProductMapDemo {

  def main(args: Array[String]): Unit = {

    // Product quantities
    val quantities = Map(
      "Laptop" -> 5,
      "Mouse" -> 20,
      "Keyboard" -> 10,
      "Monitor" -> 8,
      "Headphones" -> 15
    )

    // Product prices
    val prices = Map(
      "Laptop" -> 60000.0,
      "Mouse" -> 800.0,
      "Keyboard" -> 1500.0,
      "Monitor" -> 12000.0,
      "Headphones" -> 2500.0
    )

    println("===== PRODUCT QUANTITIES =====")
    quantities.foreach {
      case (product, quantity) =>
        println(product + " -> " + quantity)
    }

    println("\n===== PRODUCT PRICES =====")
    prices.foreach {
      case (product, price) =>
        println(product + " -> ₹" + price)
    }

    // Calculate total value for each product
    val productValues = quantities.map {
      case (product, quantity) =>
        val price = prices(product)
        product -> (quantity * price)
    }

    println("\n===== TOTAL VALUE BY PRODUCT =====")
    productValues.foreach {
      case (product, value) =>
        println(product + " -> ₹" + value)
    }

    // Calculate total inventory value
    val totalInventoryValue = productValues.values.reduce((a, b) => a + b)

    println("\n===== TOTAL INVENTORY VALUE =====")
    println("₹" + totalInventoryValue)
  }
}
