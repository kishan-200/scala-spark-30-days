# Day 22 — Batch Mini Project

## E-Commerce Daily Sales Pipeline

## Overview

This project implements an end-to-end batch processing pipeline using Apache Spark.

The scenario is an e-commerce daily sales pipeline where raw transaction data is cleaned, enriched with customer and product information, aggregated into sales metrics, and stored as partitioned Parquet output.

## Objectives

- Read raw transaction data.
- Read customer and product reference data.
- Clean invalid transaction records.
- Join transaction data with customer data.
- Join transaction data with product data.
- Calculate transaction revenue.
- Aggregate daily sales metrics.
- Write partitioned Parquet output.
- Read the Parquet output back for verification.

## Pipeline

```text
Raw Transactions
       |
       v
Clean Invalid Records
       |
       v
Join Customer Data
       |
       v
Join Product Data
       |
       v
Calculate Revenue
       |
       v
Aggregate Daily Sales
       |
       v
Partitioned Parquet Output
