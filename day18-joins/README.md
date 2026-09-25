# Day 18 — Spark Joins

## Overview

This project demonstrates different join operations in Apache Spark using Scala and Spark SQL.

The project joins customer, order, and payment datasets to build a combined order analytics report.

## Objectives

- Implement inner join.
- Implement left join.
- Implement right join.
- Implement full outer join.
- Handle ambiguous column names using aliases.
- Handle NULL values after left joins.
- Understand Shuffle Sort Merge Join.
- Join orders, customers, and payments.

## Project Structure

```text
day18-joins/
├── .gitignore
├── README.md
├── build.sbt
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   └── payments.csv
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── JoinsApp.scala
