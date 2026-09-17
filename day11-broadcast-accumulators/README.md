# Day 11 — Broadcast and Accumulators

## Objective

This project demonstrates Apache Spark broadcast variables and accumulators using Scala and RDDs.

The implementation covers:

- Broadcasting a small product reference map
- Using an accumulator to count bad records
- Understanding why normal driver variables should not be used for distributed updates
- Combining broadcast data with RDD processing
- Validating transactions against a small product master table

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
day11-broadcast-accumulators/
├── data/
│   ├── product_master.txt
│   └── transactions.txt
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── BroadcastAccumulatorApp.scala
├── project/
│   └── build.properties
├── .gitignore
├── build.sbt
└── README.md
