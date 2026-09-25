# Day 20 — File Formats and Output

## Overview

This project demonstrates how Apache Spark reads and writes different file formats and how output can be partitioned for efficient storage and querying.

The project uses a daily sales dataset and demonstrates CSV, JSON, and Parquet output, along with partitioned output by year, month, and day.

## Objectives

- Read CSV data using Spark.
- Write data as CSV.
- Write data as JSON.
- Write data as Parquet.
- Create partitioned output.
- Understand Spark file layout.
- Understand the relationship between partitions and output files.
- Practice `repartition()` before writing.
- Store daily sales partitioned by year, month, and day.

## Scenario

The scenario represents a daily sales processing pipeline:

```text
Daily Sales CSV
      ↓
Spark DataFrame
      ↓
Date Transformation
      ↓
 ┌────┼───────────┐
 ↓    ↓           ↓
CSV  JSON      Parquet
                ↓
       Partition by year/month/day
