# Day 21 — Spark Catalog

## Overview

This project demonstrates the Spark Catalog API using a small hotel booking analytics scenario.

The application creates a database, registers a temporary view, creates a table, queries catalog metadata, and performs hotel booking analytics using Spark SQL.

## Objectives

- List databases using Spark Catalog.
- Create a Spark database.
- Create temporary views.
- Query temporary views using Spark SQL.
- Register a DataFrame as a table.
- List tables from a database.
- Inspect table and column metadata.
- Check whether a table exists.
- Perform hotel revenue analysis.
- Inspect a Spark SQL query plan.

## Scenario

A small hotel booking analytics database is created to analyze hotel reservations and revenue.

```text
Hotel Booking CSV
       |
       v
Spark DataFrame
       |
       +--------------------+
       |                    |
       v                    v
Temporary View          Catalog Table
       |                    |
       v                    v
   Spark SQL          Hotel Analytics
