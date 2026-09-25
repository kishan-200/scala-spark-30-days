# Day 16 — Spark Aggregations

## Overview

This project focuses on aggregation operations in Apache Spark using Scala DataFrames.

The implementation uses a hospital revenue dataset to generate department-wise revenue metrics and demonstrates how Spark can perform grouped and filtered aggregations efficiently.

## Objectives

- Practice `count`, `sum`, `avg`, `min`, and `max`.
- Use `groupBy` with one or multiple columns.
- Perform department-wise aggregation.
- Apply HAVING-like filtering after aggregation.
- Generate hospital department revenue metrics using Spark DataFrame APIs.

## Project Structure

```text
day16-aggregations/
├── .gitignore
├── README.md
├── build.sbt
├── data/
│   └── hospital_revenue.csv
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── AggregationsApp.scala
