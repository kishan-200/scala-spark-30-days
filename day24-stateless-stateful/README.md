# Day 24 — Stateless vs Stateful Streaming

## Objective

The objective of this lab is to understand the difference between stateless and stateful processing using Spark Streaming DStreams.

The project maintains transaction counts for bank accounts and demonstrates:

- Stateless processing
- Stateful processing
- Current-batch results
- Running accumulated counts
- Micro-batch streaming

---

## Scenario

Bank transactions are received continuously through a socket.

Each transaction has the following format:

account_id,transaction_amount

Example:

A101,500
A102,1000
A101,200
A103,700

The application calculates:

1. Current transaction count in each batch
2. Running transaction count for each account across batches

---

## Technologies Used

- Scala
- Apache Spark
- Spark Streaming
- DStreams
- SBT
- Java 17
- WSL2 / Ubuntu

---

## Project Structure

```text
day24-stateless-stateful/
│
├── README.md
├── build.sbt
├── .gitignore
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── StatelessStatefulApp.scala
