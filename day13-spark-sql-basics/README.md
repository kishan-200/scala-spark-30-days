# Day 13 – Spark SQL Basics

## Objective

Learn the fundamentals of Spark SQL and DataFrame operations using CSV and JSON data.

## Topics Covered

- Creating DataFrames from CSV and JSON
- Schema inspection using `printSchema()`
- Selecting columns using `select()`
- Filtering records using `filter()`
- Creating derived columns using `withColumn()`
- Column expressions using Spark SQL functions
- Temporary views
- Spark SQL queries
- `GROUP BY`, `ORDER BY`, and `JOIN`
- Customer analytics using Spark SQL

## Project Structure

```text
day13-spark-sql-basics/
├── README.md
├── build.sbt
├── .gitignore
├── data/
│   ├── customers.csv
│   └── customer_preferences.json
├── project/
│   └── build.properties
├── screenshots/
└── src/
    └── main/
        └── scala/
            └── SparkSQLBasicsApp.scala
