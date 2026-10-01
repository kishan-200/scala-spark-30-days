# Day 26 — Real-Time Healthcare Streaming

## Objective

Build a real-time healthcare monitoring application using
Apache Spark DStreams.

The application processes patient vital readings and detects
abnormal values in real time.

## Technologies

- Scala
- Apache Spark
- Spark Streaming DStreams
- sbt
- Netcat

## Patient Vital Schema

Each event contains:

patient_id,heart_rate,spo2,temperature,systolic_bp,diastolic_bp

## Normal Thresholds

- Heart Rate: 60–100
- SpO2: >= 95
- Temperature: 36.0–37.5
- Systolic BP: 90–140
- Diastolic BP: 60–90

## Concepts Demonstrated

### DStreams

Patient events are received through a socket stream.

### Broadcast Variables

Normal vital thresholds are broadcast to Spark executors.

### Accumulator

An accumulator counts abnormal vital readings.

### Stateful Processing

Running abnormal-reading counts are maintained for each patient.

### Window Processing

A 20-second window is used to identify repeated abnormal
readings.

## Input Example

```text
P001,72,98,36.7,120,80
P003,45,89,39.1,150,95
