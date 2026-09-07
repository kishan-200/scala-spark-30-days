object ForComprehensionDemo {

  def main(args: Array[String]): Unit = {

    val students = List(
      ("Rahul", 85),
      ("Kishan", 90),
      ("Anil", 35),
      ("Priya", 92)
    )

    val passedStudents = for {
      student <- students
      if student._2 >= 40
    } yield student

    println("===== ALL STUDENTS =====")

    students.foreach(println)

    println("\n===== PASSED STUDENTS =====")

    passedStudents.foreach(println)
  }
}
