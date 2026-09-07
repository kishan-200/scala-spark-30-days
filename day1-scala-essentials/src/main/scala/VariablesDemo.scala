object VariablesDemo {

  def main(args: Array[String]): Unit = {

    // val - immutable variable
    val name = "Kishan"

    // var - mutable variable
    var marks = 80

    // lazy val - calculated only when first accessed
    lazy val result = {
      println("Calculating result...")
      if (marks >= 40) "PASS" else "FAIL"
    }

    println("Name: " + name)
println("Marks: " + marks)

marks = 90

println("Updated Marks: " + marks)

println("Before accessing result")

println("Result: " + result)
  }
}
