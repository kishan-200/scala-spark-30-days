# Day 1 — Scala Essentials

Part of my **30-Day Scala + Apache Spark Practice Journey**.

## Topics Covered

- `val`, `var`, and `lazy val`
- Immutable Collections
- List, Vector, Set, and Map
- For-Comprehension and `yield`
- Scala Traits
- Code Reusability
- Collection Operations
- Student Grade Processing

## Programs

| Program | Description |
|---|---|
| `VariablesDemo.scala` | Demonstrates `val`, `var`, and `lazy val` |
| `CollectionsDemo.scala` | Demonstrates List, Vector, Set, and Map |
| `ForComprehensionDemo.scala` | Demonstrates for-comprehension and `yield` |
| `LoggerDemo.scala` | Demonstrates traits and code reuse |
| `StudentGradeProcessor.scala` | Practical student grade processing application |

## Student Grade Processor

The mini project processes student marks, calculates grades, identifies passed and failed students, and generates a summary using **Scala collections**.

### Sample Output

```text
===== STUDENT GRADE REPORT =====
Student: Rahul | Marks: 85 | Grade: B
Student: Kishan | Marks: 90 | Grade: A
Student: Anil | Marks: 35 | Grade: F
Student: Priya | Marks: 92 | Grade: A
Student: Sneha | Marks: 68 | Grade: D

===== SUMMARY =====
Total Students: 5
Passed Students: 4
Failed Students: 1
