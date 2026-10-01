# Day 27 — Real-Time Banking Project

## Objective

Build a real-time banking transaction monitoring application using
Apache Spark Streaming and Scala.

## Requirements

- Design a transaction event schema.
- Aggregate transactions by account.
- Detect suspicious transaction bursts using windows.
- Join transactions with small branch/risk reference data.
- Use cache/persist and partitioning.
- Explain how the application can run on YARN.

## Technologies

- Scala
- Apache Spark Streaming
- Spark DStreams
- sbt
- Netcat
- Linux/WSL2

## Project Structure

```text
day27-real-time-banking/
├── README.md
├── build.sbt
├── data/
│   ├── branch_risk.txt
│   └── transactions.txt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── BankingStreamingApp.scala
