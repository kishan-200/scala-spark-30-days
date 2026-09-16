# Day 9 — Pair RDD

Part of my **30-Day Scala + Apache Spark Practice Journey**.

This project demonstrates **Pair RDDs** in Apache Spark and their use in key-value data processing, aggregation, transformation, and transaction analysis.

The implementation covers `reduceByKey`, `groupByKey`, `mapValues`, revenue aggregation by product and department, and bank transaction aggregation by account ID.

---

## Objective

The objectives of this project are:

- Create key-value Pair RDDs.
- Understand and use `reduceByKey`.
- Understand and use `groupByKey`.
- Understand and use `mapValues`.
- Calculate total revenue by product.
- Calculate total revenue by department.
- Compare `reduceByKey` and `groupByKey`.
- Aggregate bank transactions by account ID.
- Understand how Pair RDDs are used for distributed aggregation.

---

## Technologies Used

- **Scala:** 2.12.18
- **Apache Spark:** 3.5.3
- **Spark Core**
- **Spark SQL**
- **sbt**
- **Java:** 17
- **Execution Mode:** Local mode with 4 cores
- **Environment:** WSL2 Ubuntu

---

## Project Structure

```text
day9-pair-rdd/
├── data/
│   ├── sales.txt
│   └── bank_transactions.txt
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── PairRDDApp.scala
├── project/
│   └── build.properties
├── build.sbt
├── .gitignore
└── README.md
