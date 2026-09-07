object StudentGradeProcessor {

  // --------------------------------------------------
  // Student data
  // --------------------------------------------------

  val students = List(
    ("Rahul", 85),
    ("Kishan", 90),
    ("Anil", 35),
    ("Priya", 92),
    ("Sneha", 68)
  )


  // --------------------------------------------------
  // Calculate grade
  // --------------------------------------------------

  def calculateGrade(marks: Int): String = {

    if (marks >= 90)
      "A"
    else if (marks >= 80)
      "B"
    else if (marks >= 70)
      "C"
    else if (marks >= 60)
      "D"
    else if (marks >= 40)
      "E"
    else
      "F"
  }


  // --------------------------------------------------
  // Process students using for-comprehension
  // --------------------------------------------------

  def processStudents(): List[(String, Int, String)] = {

    for {
      (name, marks) <- students
    } yield {

      val grade = calculateGrade(marks)

      (name, marks, grade)
    }
  }


  // --------------------------------------------------
  // Display report
  // --------------------------------------------------

  def displayReport(
      processedStudents: List[(String, Int, String)]
  ): Unit = {

    println("\n===== STUDENT GRADE REPORT =====")

    processedStudents.foreach {

      case (name, marks, grade) =>

        println(
          s"Student: $name | Marks: $marks | Grade: $grade"
        )
    }
  }


  // --------------------------------------------------
  // Display summary
  // --------------------------------------------------

  def displaySummary(
      processedStudents: List[(String, Int, String)]
  ): Unit = {

    val passedStudents =
      processedStudents.filter {

        case (_, marks, _) =>
          marks >= 40
      }

    val failedStudents =
      processedStudents.filter {

        case (_, marks, _) =>
          marks < 40
      }


    println("\n===== SUMMARY =====")

    println(
      "Total Students: " +
        processedStudents.size
    )

    println(
      "Passed Students: " +
        passedStudents.size
    )

    println(
      "Failed Students: " +
        failedStudents.size
    )
  }


  // --------------------------------------------------
  // Main method
  // --------------------------------------------------

  def main(args: Array[String]): Unit = {

    println("======================================")
    println("       STUDENT GRADE PROCESSOR")
    println("======================================")

    val processedStudents = processStudents()

    displayReport(processedStudents)

    displaySummary(processedStudents)
  }
}
