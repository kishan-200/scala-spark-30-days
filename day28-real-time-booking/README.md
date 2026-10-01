# Day 28 — Real-Time Booking Project

## Objective

Process real-time hotel, flight and bus booking events using Spark Streaming.

## Concepts Used

- Real-time booking event processing
- Booking and cancellation events
- Stateful processing
- Occupancy calculation
- Availability calculation
- Pair RDD
- Window operations
- Broadcast reference data
- Accumulator
- Spark SQL reporting

## Event Format

booking_id,user_id,resource_id,event_type,quantity,amount

Example:

B001,U001,H001,BOOK,2,2000

## Reference Data

resource_id,resource_name,resource_type,capacity

Example:

H001,Hotel Sunrise,Hotel,100

## Architecture

Booking Events
      |
      v
Socket Stream
      |
      v
Spark Streaming
      |
      +----> Pair RDD
      |
      +----> Stateful Booking State
      |
      +----> Window Operations
      |
      +----> Broadcast Reference Data
      |
      +----> Occupancy / Availability
      |
      +----> Spark SQL Report
      |
      v
Console Output
