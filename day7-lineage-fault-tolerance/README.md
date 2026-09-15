# Day 7 — RDD Immutability, Lineage and Fault Tolerance

Part of my **30-Day Scala + Apache Spark Practice Journey**.

## Objective

This project demonstrates how Apache Spark RDDs provide:

- Immutability
- RDD lineage
- Lazy evaluation
- Partition-based processing
- Fault tolerance through recomputation

## Topics Covered

### 1. RDD Immutability

RDDs are immutable distributed datasets. A transformation does not modify the existing RDD. Instead, it creates a new RDD.

Example:

```scala
val numbers = sc.textFile("data/numbers.txt", 4)

val evenNumbers = numbers
  .map(_.toInt)
  .filter(_ % 2 == 0)
