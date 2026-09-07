object SalesOperationsDemo {

  def main(args: Array[String]): Unit = {

    // Sales amounts for different transactions
    val sales = List(1200, 800, 1500, 600, 2000, 450)

    println("===== ORIGINAL SALES =====")
    println(sales)

    // map: Add 10% tax to every sale
    val salesWithTax = sales.map(amount => amount * 1.10)

    println("\n===== SALES WITH 10% TAX =====")
    println(salesWithTax)

    // filter: Select sales greater than 1000
    val highValueSales = sales.filter(amount => amount > 1000)

    println("\n===== HIGH VALUE SALES (> 1000) =====")
    println(highValueSales)

    // flatMap: Split each sale into smaller values
    val saleBreakdown = sales.flatMap { amount =>
      List(amount, amount / 2)
    }

    println("\n===== SALE BREAKDOWN USING flatMap =====")
    println(saleBreakdown)

    // reduce: Calculate total sales
    val totalSales = sales.reduce((a, b) => a + b)

    println("\n===== TOTAL SALES =====")
    println(totalSales)
  }
}
