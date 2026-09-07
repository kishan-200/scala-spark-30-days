object CollectionsDemo {

  def main(args: Array[String]): Unit = {

    // List
    val students = List(
      "Rahul",
      "Kishan",
      "Anil",
      "Priya"
    )

    // Vector
    val marks = Vector(
      85,
      90,
      78,
      92
    )

    // Set
    val subjects = Set(
      "Scala",
      "Spark",
      "SQL",
      "Scala"
    )

    // Map
    val studentMarks = Map(
      "Rahul" -> 85,
      "Kishan" -> 90,
      "Anil" -> 78,
      "Priya" -> 92
    )

    println("===== LIST =====")
    println(students)

    println("\n===== VECTOR =====")
    println(marks)

    println("\n===== SET =====")
    println(subjects)

    println("\n===== MAP =====")
    println(studentMarks)
   println("\n===== COLLECTION OPERATIONS =====")

println("First student: " + students.head)

println("Third mark: " + marks(2))

println("Number of unique subjects: " + subjects.size)

println("Kishan's marks: " + studentMarks("Kishan"))
  }
}
