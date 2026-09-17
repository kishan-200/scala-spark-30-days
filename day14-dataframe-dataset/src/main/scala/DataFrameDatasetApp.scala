import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  designation: String,
  basic_salary: Double,
  bonus: Double,
  tax: Double
)

case class Payroll(
  employee_id: String,
  name: String,
  department: String,
  designation: String,
  basic_salary: Double,
  bonus: Double,
  tax: Double,
  gross_salary: Double,
  net_salary: Double
)

object DataFrameDatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrame and Dataset")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("===== DAY 14: DATAFRAME AND DATASET =====")

    // --------------------------------------------------
    // 1. Create DataFrame from CSV
    // --------------------------------------------------

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n===== EMPLOYEE DATAFRAME =====")
    employeeDF.show(false)

    println("\n===== DATAFRAME SCHEMA =====")
    employeeDF.printSchema()

    // --------------------------------------------------
    // 2. DataFrame -> Dataset
    // --------------------------------------------------

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println("\n===== DATAFRAME TO DATASET =====")
    employeeDS.show(false)

    println("\nDataset type: Dataset[Employee]")

    // --------------------------------------------------
    // 3. Typed Dataset operations
    // --------------------------------------------------

    val engineeringEmployees =
      employeeDS.filter(_.department == "Engineering")

    println("\n===== ENGINEERING EMPLOYEES =====")
    engineeringEmployees.show(false)

    // --------------------------------------------------
    // 4. Typed payroll calculation
    // --------------------------------------------------

    val payrollDS: Dataset[Payroll] =
      employeeDS.map { employee =>

        val grossSalary =
          employee.basic_salary + employee.bonus

        val netSalary =
          grossSalary - employee.tax

        Payroll(
          employee.employee_id,
          employee.name,
          employee.department,
          employee.designation,
          employee.basic_salary,
          employee.bonus,
          employee.tax,
          grossSalary,
          netSalary
        )
      }

    println("\n===== TYPED PAYROLL DATASET =====")
    payrollDS.show(false)

    // --------------------------------------------------
    // 5. Dataset -> DataFrame
    // --------------------------------------------------

    val payrollDF =
      payrollDS.toDF()

    println("\n===== DATASET TO DATAFRAME =====")
    payrollDF.show(false)

    println("\n===== PAYROLL DATAFRAME SCHEMA =====")
    payrollDF.printSchema()

    // --------------------------------------------------
    // 6. Payroll report using DataFrame API
    // --------------------------------------------------

    println("\n===== PAYROLL REPORT =====")

    payrollDF
      .select(
        "employee_id",
        "name",
        "department",
        "gross_salary",
        "net_salary"
      )
      .orderBy(desc("net_salary"))
      .show(false)

    // --------------------------------------------------
    // 7. Department-wise payroll
    // --------------------------------------------------

    println("\n===== DEPARTMENT-WISE PAYROLL =====")

    payrollDF
      .groupBy("department")
      .agg(
        count("*").alias("employees"),
        round(sum("gross_salary"), 2).alias("total_gross_salary"),
        round(sum("net_salary"), 2).alias("total_net_salary"),
        round(avg("net_salary"), 2).alias("average_net_salary")
      )
      .orderBy(desc("total_net_salary"))
      .show(false)

    // --------------------------------------------------
    // 8. Type safety demonstration
    // --------------------------------------------------

    println("\n===== TYPE SAFETY =====")
    println("Dataset[Employee] provides a typed representation")
    println("of employee records.")
    println("Employee fields are accessed using Scala properties.")
    println("Example: employee.basic_salary")
    println("Incorrect field types can be detected during compilation.")

    // --------------------------------------------------
    // 9. RDD vs DataFrame vs Dataset
    // --------------------------------------------------

    println("\n===== RDD vs DATAFRAME vs DATASET =====")
    println("RDD:")
    println("- Low-level distributed collection")
    println("- No built-in schema")
    println("- Flexible but less optimized")

    println("\nDataFrame:")
    println("- Distributed table with named columns")
    println("- Schema-based")
    println("- Optimized by Catalyst and Spark SQL")

    println("\nDataset:")
    println("- Typed distributed collection")
    println("- Combines type safety with Spark SQL optimization")
    println("- Uses case classes for structured records")

    // --------------------------------------------------
    // 10. Catalyst optimization
    // --------------------------------------------------

    println("\n===== CATALYST OPTIMIZATION =====")
    println("Spark SQL uses the Catalyst optimizer")
    println("to optimize DataFrame and Dataset queries.")
    println("It analyzes the logical plan and creates")
    println("an optimized execution plan.")

    println("\n===== DAY 14 COMPLETE =====")

    spark.stop()
  }
}
