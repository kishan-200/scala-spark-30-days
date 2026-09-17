# Day 12 — Cache and Persist

## Objective

This project demonstrates Apache Spark caching and persistence using Scala and RDDs.

The implementation covers:

- Caching an RDD reused by multiple actions
- Comparing `cache()` and `persist()`
- Experimenting with different storage levels
- Understanding when caching can hurt performance
- Reusing a cleaned transaction dataset for three reports

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
day12-cache-persist/
├── data/
│   └── transactions.txt
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── CachePersistApp.scala
├── project/
│   └── build.properties
├── .gitignore
├── build.sbt
└── README.md
