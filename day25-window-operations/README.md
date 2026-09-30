# Day 25 — Window Operations

## Objective

Practice Spark Streaming window operations using Scala and DStreams.

## Concepts Covered

- Batch interval
- Window size
- Sliding interval
- `countByWindow`
- `reduceByKeyAndWindow`
- Rolling sales totals
- Transaction monitoring over a time window

## Scenario

A streaming application receives transaction data continuously.

The application:

1. Counts transactions received during the last 30 seconds.
2. Calculates rolling sales totals for each account.
3. Uses a 10-second sliding interval to update the results.
4. Demonstrates how window operations process recent streaming data.

## Configuration

| Configuration | Value |
|---|---:|
| Batch Interval | 5 seconds |
| Window Size | 30 seconds |
| Sliding Interval | 10 seconds |
| Input | Socket |
| Host | localhost |
| Port | 9999 |

## Input Format

The application expects transaction data in the following format:

```text
account_id,amount
