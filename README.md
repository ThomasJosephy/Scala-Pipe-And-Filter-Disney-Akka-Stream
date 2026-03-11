# Disneyland Paris Queue Analysis with Akka Streams

[cite_start]A Pipe-and-Filter architecture implemented in Scala 3 using Akka Streams to process and analyze ride wait times at Disneyland Paris[cite: 222, 260]. This project was developed as a solution for the Software Architectures course.

## Features

[cite_start]The stream processing pipeline extracts meaningful insights from the dataset by answering six specific questions[cite: 226, 227]. 

* [cite_start]**Data Parsing:** The raw CSV lines are parsed into a stream of typed objects[cite: 237].
* **Stream Routing:** Uses a GraphDSL Broadcast shape to route the parsed records concurrently across six customized logical flows.
* **Analytics:** Computes total rides, top 5 mean wait times, open percentage per ride, largest variance in wait times, park-wide hourly average, and dynamic skip-the-line pricing.
* [cite_start]**Backpressure & Throttling:** A buffer with a capacity of 10 elements is applied along with a backpressure strategy[cite: 243, 244]. [cite_start]A throttle restricts the processing speed to 5000 elements per second[cite: 245].
* [cite_start]**File Output:** The output of each custom flow is written to an external text file containing the corresponding results[cite: 248].

## Prerequisites

* Scala 3 and sbt must be installed.
* [cite_start]The `queuetimes.csv` input file must be located in the `src/main/resources/` directory[cite: 236].
* Dependencies for `akka-stream` and `akka-stream-alpakka-csv` must be included in your `build.sbt`.

## How to Run

1. Clone this repository and navigate to the root folder.
2. Ensure the dataset is correctly placed in the `resources` directory.
3. Run the application using sbt:
   ```bash
   sbt run
