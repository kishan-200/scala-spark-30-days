# Day 14 – DataFrame and Dataset

## Objective

Learn how Spark DataFrames and Datasets work together using a typed employee payroll pipeline.

## Topics Covered

- Creating a Scala `case class`
- Creating a DataFrame from CSV
- Converting DataFrame to Dataset
- Typed Dataset operations
- Creating a typed payroll Dataset
- Converting Dataset back to DataFrame
- Comparing RDD, DataFrame, and Dataset
- Understanding type safety
- Understanding Catalyst optimization
- Building an employee payroll analytics report

## Project Structure

```text
day14-dataframe-dataset/
├── README.md
├── build.sbt
├── .gitignore
├── data/
│   └── employees.csv
├── project/
│   └── build.properties
├── screenshots/
└── src/
    └── main/
        └── scala/
            └── DataFrameDatasetApp.scala
