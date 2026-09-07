trait Logger {

  def log(message: String): Unit = {
    println("[LOG] " + message)
  }
}


class StudentService extends Logger {

  def processStudent(name: String): Unit = {
    log("Processing student: " + name)
  }
}


class GradeService extends Logger {

  def processGrade(student: String, marks: Int): Unit = {
    log("Processing grade for " + student + ": " + marks)
  }
}


object LoggerDemo {

  def main(args: Array[String]): Unit = {

    val studentService = new StudentService()
    val gradeService = new GradeService()

    println("===== LOGGER DEMO =====")

    studentService.processStudent("Kishan")

    gradeService.processGrade("Kishan", 90)
  }
}
