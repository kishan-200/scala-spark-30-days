# Day 4 — RDD Creation

Part of my **30-Day Scala + Apache Spark Practice Journey**.

## Topics Covered

- Creating RDDs from Scala collections
- Creating RDDs from text files
- `map`, `filter`, and `flatMap`
- Calculating total sales using RDDs
- Inspecting RDD partitions
- Understanding default parallelism
- Processing customer data across multiple partitions

## Programs

| Program | Description |
|---|---|
| `RDDCreationApp.scala` | Demonstrates RDD creation and transformations |
| `CustomerPartitionApp.scala` | Demonstrates customer data partitioning |

## RDD Creation

An RDD was created from a Scala collection using:

```scala
sc.parallelize(1 to 10)
