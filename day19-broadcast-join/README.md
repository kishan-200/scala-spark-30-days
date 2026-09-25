# Day 19 — Broadcast Join

## Overview

This project demonstrates Broadcast Join in Apache Spark using Scala and Spark SQL.

The scenario represents a large transaction dataset joined with a small branch master dataset.

The small branch reference data is broadcast to the executors to avoid unnecessary shuffling of the large transaction dataset.

## Objectives

- Create a large fact DataFrame.
- Create a small reference DataFrame.
- Perform a Broadcast Join.
- Understand when Broadcast Join is appropriate.
- Compare Broadcast Join with Shuffle Sort Merge Join.
- Generate branch-wise transaction metrics.
- Inspect Spark execution plans.

## Scenario

The project models a banking transaction system:

```text
Large Transactions Dataset
          +
Small Branch Master Dataset
          ↓
     Broadcast Join
          ↓
Transaction Analytics Report
