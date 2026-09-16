# Day 10 — Spark Partitioning

## Objective

This project demonstrates Apache Spark partitioning concepts using Scala and RDDs.

The implementation covers:

- Inspecting partition counts
- Increasing partitions using `repartition`
- Decreasing partitions using `coalesce`
- Understanding when increasing or decreasing partitions is useful
- Using `partitionBy` with a Pair RDD
- Inspecting partition contents
- Optimizing a dataset suffering from too few partitions

---

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Core
- sbt
- Java 17
- Ubuntu / WSL2

---

## Project Structure

```text
day10-partitioning/
├── data/
│   └── sales.txt
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── PartitioningApp.scala
├── project/
│   └── build.properties
├── .gitignore
├── build.sbt
└── README.md
