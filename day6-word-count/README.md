# Day 6 — Word Count

Part of my **30-Day Scala + Apache Spark Practice Journey**.

## Objective

The objective of Day 6 is to implement the classic **Word Count** problem using Apache Spark RDDs and understand how Spark performs key-value aggregation.

This exercise focuses on:

- Implementing Word Count using RDDs
- Understanding `flatMap → map → reduceByKey`
- Performing case-insensitive word counting
- Removing punctuation
- Ignoring empty words
- Finding the top 10 most frequently occurring words
- Applying Word Count to application log data

---

## Topics Covered

### Spark RDD Operations

- `flatMap`
- `map`
- `reduceByKey`
- `filter`
- `sortBy`
- `take`

### Data Processing Concepts

- Key-value pairs
- Aggregation by key
- Case normalization
- Data cleaning
- Punctuation removal
- Empty-value filtering
- Frequency analysis

### Practical Scenario

**Find the top 10 most frequent words in application logs.**

---

## Project Structure

```text
day6-word-count/
│
├── data/
│   └── application.log
│
├── screenshots/
│
├── src/
│   └── main/
│       └── scala/
│           └── WordCountApp.scala
│
├── project/
│   └── build.properties
│
├── build.sbt
├── .gitignore
└── README.md
