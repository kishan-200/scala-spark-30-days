# Day 15 – UDF Practice

## Objective

Learn how to create and use User Defined Functions (UDFs) in Spark for custom business logic.

This project implements a customer risk classification pipeline using transaction values.

## Topics Covered

- Creating a Scala UDF
- Salary band classification using a UDF
- Customer risk classification
- Adding calculated columns using `withColumn()`
- Comparing UDFs with built-in Spark functions
- Registering a UDF with the Spark session/catalog
- Using a registered UDF in Spark SQL
- Creating a customer risk analytics report

## Project Structure

```text
day15-udf-practice/
├── README.md
├── build.sbt
├── .gitignore
├── data/
│   └── customer_transactions.csv
├── project/
│   └── build.properties
├── screenshots/
└── src/
    └── main/
        └── scala/
            └── UDFPracticeApp.scala
