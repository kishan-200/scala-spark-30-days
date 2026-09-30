# Day 23 — DStreams Basics

## Application Log Streaming

## Overview

This project demonstrates basic Spark Streaming using DStreams.

The application reads application log messages from a socket, processes the incoming data using `flatMap`, `filter`, `map`, and `reduceByKey`, and counts ERROR messages in every micro-batch.

## Scenario

An application continuously produces log messages such as:

- INFO
- ERROR
- WARNING

The Spark Streaming application monitors these logs and counts ERROR messages every 5 seconds.

## Architecture

```text
Application Logs
       |
       v
Socket Stream
       |
       v
DStream
       |
       v
flatMap
       |
       v
filter ERROR
       |
       v
map
       |
       v
reduceByKey
       |
       v
ERROR Count
       |
       v
Every 5 Seconds
