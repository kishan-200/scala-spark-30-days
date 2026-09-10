# Day 5 — Transformations and Actions

Part of my **30-Day Scala + Apache Spark Practice Journey**.

## Objective

The objective of Day 5 is to understand how Spark processes data using **RDD transformations and actions**.

This day focuses on:

- Understanding Spark transformations
- Understanding Spark actions
- Understanding lazy evaluation
- Practicing common RDD operations
- Building a simple log analyzer
- Counting `ERROR` messages from application logs

---

## Topics Covered

### Transformations

- `map`
- `filter`
- `flatMap`
- `distinct`
- `union`

### Actions

- `count`
- `collect`
- `first`
- `take`
- `reduce`

### Spark Concepts

- Transformations vs Actions
- Lazy evaluation
- RDD processing
- Log analysis
- Local execution using multiple cores

---

# Project Structure

```text
day5-transformations-actions/
│
├── data/
│   └── application.log
│
├── screenshots/
│
├── src/
│   └── main/
│       └── scala/
│           └── TransformationsActionsApp.scala
│
├── project/
│   └── build.properties
│
├── build.sbt
├── .gitignore
└── README.md
