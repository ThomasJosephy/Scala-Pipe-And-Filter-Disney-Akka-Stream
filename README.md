# Disneyland Paris Queue Analysis

A data processing pipeline built with Scala 3 and Akka Streams to analyze ride wait times at Disneyland Paris. It uses a Pipe-and-Filter architecture to parse, analyze, and output metrics concurrently.

## Features

* **Stream Parsing:** Reads and parses raw CSV data using Alpakka CSV.
* **Concurrent Processing:** Uses Akka Streams `GraphDSL` with a Broadcast shape to route records through six distinct analytical flows simultaneously.
* **Custom Analytics:** Computes total rides, top 5 mean wait times, open percentage per ride, highest wait time variance, park-wide hourly average, and dynamic pricing.
* **Traffic Control:** Implements buffers (capacity of 10) with backpressure strategies and limits processing speed to 5000 elements per second.
* **File I/O:** Automatically routes the results of each analytical flow to dedicated text files via Sinks.

## Prerequisites

* Scala 3 and sbt installed on your machine.
* The `queuetimes.csv` dataset must be placed in the `src/main/resources/` directory.
* Required dependencies in your `build.sbt`: `akka-stream` and `akka-stream-alpakka-csv`.

## How to Run

1. Clone the repository and navigate to the project root directory.
2. Ensure the output directory exists at `src/main/resources/results/`.
3. Execute the application using sbt:
   ```bash
   sbt run
