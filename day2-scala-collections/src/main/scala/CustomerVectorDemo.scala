object CustomerVectorDemo {

  def main(args: Array[String]): Unit = {

    // Vector of customer records
    val customers = Vector(
      ("C001", "Rahul", "Bangalore"),
      ("C002", "Kishan", "Hyderabad"),
      ("C003", "Anil", "Chennai"),
      ("C004", "Priya", "Mumbai"),
      ("C005", "Sneha", "Delhi")
    )

    println("===== ALL CUSTOMERS =====")
    customers.foreach(println)

    // Access customer using index
    println("\n===== CUSTOMER AT INDEX 2 =====")
    println(customers(2))

    // Access customer using index safely
    println("\n===== CUSTOMER AT INDEX 0 =====")
    println(customers(0))

    // Filter customers by city
    val bangaloreCustomers =
      customers.filter(customer => customer._3 == "Bangalore")

    println("\n===== CUSTOMERS FROM BANGALORE =====")
    bangaloreCustomers.foreach(println)

    // Add a new customer
    val updatedCustomers =
      customers :+ ("C006", "Arjun", "Pune")

    println("\n===== AFTER ADDING CUSTOMER =====")
    updatedCustomers.foreach(println)

    println("\n===== TOTAL CUSTOMERS =====")
    println(updatedCustomers.size)
  }
}
