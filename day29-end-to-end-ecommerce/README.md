# Day 29 — End-to-End E-Commerce Project

## Overview

This project demonstrates an end-to-end e-commerce data processing pipeline using Apache Spark and Scala.

The pipeline processes raw e-commerce events through multiple stages:

Raw Data → Clean → Enrich → Aggregate → Reporting

The project combines RDD/Pair RDD processing with Spark DataFrame/Dataset processing and demonstrates important Spark concepts such as joins, window operations, partitioning, persistence, caching, shuffles, stages, and optimization.

---

## Objectives

- Build a raw → clean → enrich → aggregate pipeline.
- Process e-commerce events using Spark.
- Use RDD/Pair RDD for one processing component.
- Use DataFrame/Dataset for another component.
- Use UDF only where necessary.
- Join transaction/event data with product reference data.
- Apply window operations for analytical processing.
- Demonstrate partitioning.
- Use cache/persist for reused data.
- Understand Spark shuffles and stages.
- Explain optimization choices.
- Produce a production-style Spark application.

---

## Project Structure

```text
day29-end-to-end-ecommerce/
│
├── data/
│   ├── ecommerce_events.txt
│   └── product_reference.txt
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── EcommerceStreamingApp.scala
│
├── .gitignore
├── build.sbt
└── README.md
