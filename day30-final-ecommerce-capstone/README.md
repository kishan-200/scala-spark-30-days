# Day 30 – Final E-Commerce Capstone

##  Project Overview

This project is the final capstone of the **30-Day Scala + Apache Spark Practice Program**.

The goal is to combine the major Spark concepts learned throughout the previous days into one practical **E-Commerce data processing and analytics pipeline**.

The project demonstrates both:

- **Batch processing** of historical E-Commerce transactions
- **Real-time streaming** of incoming E-Commerce events

It combines RDDs, Pair RDDs, DataFrames, Spark SQL, joins, UDFs, window functions, persistence, broadcast variables, accumulators, partitioning, and Spark Streaming in one end-to-end project.

---

##  Objectives

The main objectives of this capstone are:

- Build an end-to-end E-Commerce analytics pipeline
- Process historical transaction data using Spark
- Process real-time E-Commerce events using Spark Streaming
- Demonstrate RDD and Pair RDD operations
- Use DataFrames for structured data processing
- Perform joins with product reference data
- Apply a custom Scala UDF
- Use Spark SQL for analytical reporting
- Calculate running customer spending using window functions
- Demonstrate caching/persistence
- Demonstrate broadcast variables
- Demonstrate accumulators
- Tune DataFrame partitioning
- Understand shuffle, stages, tasks, executors and DAG execution
- Prepare the project for technical interview discussion

---

# 🏗️Architecture

## Batch Processing Architecture

```text
                 Batch Transactions
                        │
                        ▼
                CSV Transaction Data
                        │
                        ▼
                   DataFrame
                        │
                        ▼
                Data Cleaning
                        │
                        ▼
                  Persist Data
                        │
             ┌──────────┴──────────┐
             │                     │
             ▼                     ▼
        Pair RDD Processing    Product Reference
             │                     │
             ▼                     ▼
       Customer Revenue          Product Data
                                   │
                                   ▼
                              DataFrame Join
                                   │
                                   ▼
                              Enriched Data
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
                   UDF          Window         Spark SQL
                    │           Analysis          │
                    └──────────────┼──────────────┘
                                   ▼
                           Analytics Reports
day30-final-ecommerce-capstone/
│
├── README.md
├── INTERVIEW_QA.md
├── build.sbt
├── .gitignore
│
├── data/
│   ├── batch_transactions.csv
│   ├── product_reference.csv
│   └── stream_events.txt
│
├── project/
│   └── build.properties
│
├── screenshots/
│
└── src/
    └── main/
        └── scala/
            ├── FinalECommerceCapstone.scala
            └── ECommerceStreamingApp.scala
