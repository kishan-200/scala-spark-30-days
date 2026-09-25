# Day 17 — Spark Window Functions

## Overview

This project demonstrates Spark SQL Window Functions using Scala and Apache Spark.

The implementation covers ranking students within each course, finding the latest policy for each customer, and comparing records using `lag` and `lead`.

## Objectives

- Use `row_number`, `rank`, and `dense_rank`.
- Partition data by course and customer.
- Find the top 3 students per course.
- Find the latest policy for each customer.
- Use `lag` to access previous records.
- Use `lead` to access following records.
- Understand how Window Functions operate within partitions.

## Project Structure

```text
day17-window-functions/
├── .gitignore
├── README.md
├── build.sbt
├── data/
│   ├── students.csv
│   └── customer_policies.csv
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── WindowFunctionsApp.scala
