# Day 8 — DAG and Spark Execution

Part of my **30-Day Scala + Apache Spark Practice Journey**.

This project demonstrates how Apache Spark converts a sequence of RDD transformations and actions into a **Directed Acyclic Graph (DAG)** and executes the computation through **jobs, stages, tasks, partitions, and shuffle operations**.

---

## Objective

The objectives of this project are:

- Create a Spark job containing multiple transformations and actions.
- Understand how Spark constructs a DAG from RDD operations.
- Identify narrow and wide transformations.
- Identify shuffle boundaries.
- Understand the relationship between jobs, stages, tasks, and partitions.
- Analyze the execution of `reduceByKey`.
- Predict the number of stages created by a `reduceByKey` pipeline.
- Inspect RDD lineage using Spark's `toDebugString`.

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
day8-dag-spark-execution/
├── data/
│   └── sales.txt
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── DAGExecutionApp.scala
├── project/
│   └── build.properties
├── build.sbt
├── .gitignore
└── README.md
