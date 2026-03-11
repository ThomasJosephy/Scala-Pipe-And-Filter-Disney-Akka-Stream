import akka.actor.ActorSystem
import akka.stream.*
import akka.stream.scaladsl.*
import akka.NotUsed
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import concurrent.duration.DurationInt

import java.nio.file.StandardOpenOption.*
import java.nio.file.{Path, Paths}

import scala.math.{max, pow}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}


case class RideQueueRecord(ride_id: String,
                           ride_name: String,
                           is_open: String,
                           wait_time: String,
                           date: String,
                           timestamp: String)

object TestStream extends App:
  implicit val actorSystem: ActorSystem = ActorSystem("ParkAnalysis")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  val resourcesFolder: String = "src/main/resources"

  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/queuetimes.csv")

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  // Parses the raw byte stream into distinct CSV lines
  val csvParsing: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()

  // Converts CSV lines into a Map using the header row as keys
  val mappingHeader: Flow[List[ByteString], Map[String, ByteString], NotUsed] =
    CsvToMap.toMap()

  // Transforms the Map into a RideQueueRecord object
  val flowAttraction: Flow[Map[String, ByteString], RideQueueRecord, NotUsed] =
    Flow[Map[String, ByteString]].map(tempMap => {
      tempMap.map(element => {
        (element._1, element._2.utf8String)
      })
    }).map(record => {
      RideQueueRecord(
        ride_id = record("ride_id"),
        ride_name = record("ride_name"),
        is_open = record("is_open"),
        wait_time = record("wait_time"),
        date = record("date"),
        timestamp = record("timestamp"),
      )
    })

/***********************************************************************
Flow Out Part
***********************************************************************/

  // Formats the total count of attractions into a readable string
  val flowOutTotalAttractions: Flow[Int, ByteString, NotUsed] = Flow[Int].map(totalAttractions => {
    ByteString(s"There is $totalAttractions attractions\n")
  })

  // Formats the list of top 5 rides with lowest wait times into a string
  val flowOutTop5MeanTime: Flow[List[(Int, Float)], ByteString, NotUsed] = Flow[List[(Int, Float)]].map( best5Rides => {
    val lines = best5Rides.map { ride =>
      val id = ride._1
      val meanTime = ride._2
      s"ID of the ride: $id, with a mean time of $meanTime."
    }

    val fullText = lines.mkString("\n")
    ByteString(fullText + "\n")
  }
  )

  // Formats the  percentage of the time the rides were open into a string
  val flowOutOpenInterval: Flow[List[(Int, Float)], ByteString, NotUsed] = Flow[List[(Int, Float)]].map(best5Rides => {
    val lines = best5Rides.map { ride =>
      val id = ride._1
      val openTimePercentage = ride._2
      s"ID of the ride: $id, was open $openTimePercentage% of the day."
    }

    val fullText = lines.mkString("\n")
    ByteString(fullText + "\n")
  }
  )

  // Formats the ride with the highest wait time variance into a string
  val flowOutHighestVariance: Flow[List[(Int, Float)], ByteString, NotUsed] = Flow[List[(Int, Float)]].map(ride => {
    var rideID = ride.head._1
    var variance = ride.head._2
    ByteString(s"The attraction $rideID have the biggest variation of waiting time in a day with a standard deviation of $variance\n")
  }
  )

  // Formats the hourly average park wait times into a string
  val flowOutParkWaitTime: Flow[List[(Int, Float)], ByteString, NotUsed] = Flow[List[(Int, Float)]].map(waitTime => {
    val lines = waitTime.map { ride =>
      val hour = ride._1
      val meanTime = ride._2
      s"The waiting time park-wide at $hour hour was $meanTime minutes."
    }

    val fullText = lines.mkString("\n")
    ByteString(fullText + "\n")
  }
  )

  // Formats the calculated fair prices per ride per hour into a string
  val flowOutFairPrices: Flow[List[(String, Int, Float, Double, String, String, Float)], ByteString, NotUsed] = Flow[List[(String, Int, Float, Double, String, String, Float)]].map(waitTime => {
    val lines = waitTime.map { ride =>
      val rideName = ride._1
      val rideID = ride._2
      val currentWaitTime = ride._3
      val meanWaitTime = ride._4
      val hour = ride._5
      val date = ride._6
      val price = ride._7
      s"The attraction $rideName with ID $rideID at hour $hour have a waiting time of $currentWaitTime minutes. The average for this hour is $meanWaitTime minutes. On this date ($date), a fair price would be $price €."
    }

    val fullText = lines.mkString("\n")
    ByteString(fullText + "\n")
  }
  )

/***********************************************************************
Flow Transformation Part
***********************************************************************/

  // Calculates the total number of unique attractions in the dataset
  val flowDifferentAttractions: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[Int](1))
          val toFlow = builder.add(flowOutTotalAttractions)

          val NumberOfAttractionsBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val NumberOfAttractionsThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val numberOfAttractions = {
            Flow[RideQueueRecord].statefulMapConcat { () =>    // code inspired from https://doc.akka.io/libraries/akka-core/current/stream/operators/Source-or-Flow/statefulMapConcat.html
              var rideSeen = Set.empty[String]
              { ride =>
                if (rideSeen.contains(ride.ride_id)) {
                  Nil
                } else {
                  rideSeen += ride.ride_id
                  ride :: Nil
                }
              }
            }
            .fold(0)((rideTotal, _) => rideTotal + 1)
            
          }

          balance ~> NumberOfAttractionsBuf ~> NumberOfAttractionsThro ~> numberOfAttractions ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

  // Identifies the 5 rides with the lowest average wait time
  val flowTop5MeanTime: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[List[(Int, Float)]](1))
          val toFlow = builder.add(flowOutTop5MeanTime)

          val Top5MeanTimeBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val Top5MeanTimeThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val Top5MeanTime = {
            Flow[RideQueueRecord]
              .filter(ride => ride.is_open == "TRUE")
              .groupBy(300, _.ride_id)
              .fold((0, 0, Set.empty[RideQueueRecord])) { (accumulator, ride) =>
                var totalOpenTime = accumulator._1
                var totalMeanTime = accumulator._2
                var uniqueLines = accumulator._3

                if (uniqueLines.contains(ride)) {
                  accumulator
                } else {
                  totalOpenTime += 1
                  totalMeanTime += ride.wait_time.toInt
                  uniqueLines += ride
                  (totalOpenTime, totalMeanTime, uniqueLines)
                }
              }
              .map { ride =>
                var meanTime = (ride._2 / ride._1).toFloat
                var rideID = ride._3.head.ride_id.toInt
                (rideID, meanTime)
              }
              .mergeSubstreams
              .fold(List.empty[(Int, Float)]) { (accumulator, ride) =>
                var meanTime = ride._2
                val updatedList = ride :: accumulator

                val sortedListRides = updatedList.sortBy(_._2)

                if (sortedListRides.size <= 5) {
                  sortedListRides
                } else {
                  val thresholdMeanTime = sortedListRides(4)._2
                  sortedListRides.filter(ride => ride._2 <= thresholdMeanTime)
                }

          }
          }
          balance ~> Top5MeanTimeBuf ~> Top5MeanTimeThro ~> Top5MeanTime ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

  // Calculates the percentage of time each ride was open during the day
  val flowOpenInterval: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[List[(Int, Float)]](1))
          val toFlow = builder.add(flowOutOpenInterval)

          val openIntervalBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val openIntervalThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val openInterval = {
            Flow[RideQueueRecord]
              .groupBy(300, _.ride_id)
              .fold((0, 0, Set.empty[RideQueueRecord])) { (accumulator, ride) =>
                var totalTime = accumulator._1
                var totalOpenTime = accumulator._2
                var uniqueLines = accumulator._3

                if (uniqueLines.contains(ride)) {
                  accumulator
                } else {
                  if (ride.is_open == "TRUE") {
                    totalOpenTime += 1
                    totalTime += 1
                    uniqueLines += ride
                    (totalTime, totalOpenTime, uniqueLines)
                  } else {
                    totalTime += 1
                    uniqueLines += ride
                    (totalTime, totalOpenTime, uniqueLines)
                  }
                }
              }
              .map { ride =>
                var totalOpen = ride._2
                var total = ride._1
                var openTimePercentage = (totalOpen.toFloat / total) * 100
                var rideID = ride._3.head.ride_id.toInt
                (rideID, openTimePercentage)
              }
              .mergeSubstreams
              .fold(List.empty[(Int, Float)]) { (accumulator, ride) =>
                val updatedList = ride :: accumulator
                updatedList
              }
          }
          balance ~> openIntervalBuf ~> openIntervalThro ~> openInterval ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

  // Identifies the ride with the largest variance in wait times
  val flowLargestVariance: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[List[(Int, Float)]](1))
          val toFlow = builder.add(flowOutHighestVariance)

          val highestVarianceBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val highestVarianceThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val highestVariance = {
            Flow[RideQueueRecord]
              .filter(ride => ride.is_open == "TRUE")
              .groupBy(300, _.ride_id)
              .fold((0, 0, 0, Set.empty[RideQueueRecord])) { (accumulator, ride) =>
                var totalTime = accumulator._1
                var totalWaitingTime = accumulator._2
                var totalSquaredWaitingTime = accumulator._3
                var uniqueLines = accumulator._4

                if (uniqueLines.contains(ride)) {
                  accumulator
                } else {
                  totalTime += 1
                  totalWaitingTime += ride.wait_time.toInt
                  totalSquaredWaitingTime += (ride.wait_time.toInt * ride.wait_time.toInt)
                  uniqueLines += ride
                  (totalTime, totalWaitingTime, totalSquaredWaitingTime, uniqueLines)
                }
              }
              .map { ride =>
                var totalIntervals = ride._1
                var totalWaiting = ride._2
                var totalSquaredWaiting = ride._3
                var rideID = ride._4.head.ride_id.toInt

                var meanWaitTime = (totalWaiting.toFloat / totalIntervals)
                var variance = ((totalSquaredWaiting.toFloat / totalIntervals) - (meanWaitTime * meanWaitTime)) * (totalIntervals.toFloat / (totalIntervals - 1))
                (rideID, variance)
              }
              .mergeSubstreams
              .fold(List.empty[(Int, Float)]) { (accumulator, ride) =>
                val updatedList = ride :: accumulator
                val highestVariance = updatedList.sortBy(-_._2)

                if (highestVariance.isEmpty) {
                  highestVariance
                } else {
                  val thresholdStandardDeviation = highestVariance.head._2
                  highestVariance.filter(ride => ride._2 >= thresholdStandardDeviation)
                }
              }

              }
          balance ~> highestVarianceBuf ~> highestVarianceThro ~> highestVariance ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

  // Calculates the average wait time across the entire park for each hour of the day
  val flowParkWaitTime: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[List[(Int, Float)]](1))
          val toFlow = builder.add(flowOutParkWaitTime)

          val parkWaitTimeBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val parkWaitTimeThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val parkWaitTime = {
            Flow[RideQueueRecord]
              .filter(ride => ride.is_open == "TRUE")
              .groupBy(300, _.timestamp.split(":")(0).toInt)
              .fold((0, 0, Set.empty[RideQueueRecord])) { (accumulator, ride) =>
                var totalTime = accumulator._1
                var totalWaitingTime = accumulator._2
                var uniqueLines = accumulator._3

                if (uniqueLines.contains(ride)) {
                  accumulator
                } else {
                  totalTime += 1
                  totalWaitingTime += ride.wait_time.toInt
                  uniqueLines += ride
                  (totalTime, totalWaitingTime, uniqueLines)
                }
              }
              .map { ride =>
                var totalIntervals = ride._1
                var totalWaiting = ride._2
                var hour = ride._3.head.timestamp.split(":")(0).toInt
                var meanWaitTime = (totalWaiting.toFloat / totalIntervals)
                (hour, meanWaitTime)
              }
              .mergeSubstreams
              .fold(List.empty[(Int, Float)]) { (accumulator, ride) =>
                val updatedList = ride :: accumulator
                val sortedList = updatedList.sortBy(_._1)
                sortedList
              }

          }
          balance ~> parkWaitTimeBuf ~> parkWaitTimeThro ~> parkWaitTime ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

  // Calculates a dynamic fair price for each ride per hour
  val flowFairPrices: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val balance = builder.add(Balance[RideQueueRecord](1))
          val merge = builder.add(Merge[List[(String, Int, Float, Double, String, String, Float)]](1))
          val toFlow = builder.add(flowOutFairPrices)

          val fairPricesBuf = Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure)
          val fairPricesThro = Flow[RideQueueRecord].throttle(5000, 1.second)

          val fairPrices = {
            Flow[RideQueueRecord]
              .filter(ride => ride.is_open == "TRUE")
              .groupBy(3000, ride => { ride.ride_id + "_" + ride.timestamp.split(":")(0) })
              .fold((0, 0, 0, Set.empty[RideQueueRecord])) { (accumulator, ride) =>
                var totalInterval = accumulator._1
                var totalMeanTimePerHour = accumulator._2
                var totalSquaredWaitingTime = accumulator._3
                var uniqueLines = accumulator._4

                if (uniqueLines.contains(ride)) {
                  accumulator
                } else {
                  totalInterval += 1
                  totalMeanTimePerHour += ride.wait_time.toInt
                  totalSquaredWaitingTime += (ride.wait_time.toInt * ride.wait_time.toInt)
                  uniqueLines += ride
                  (totalInterval, totalMeanTimePerHour, totalSquaredWaitingTime, uniqueLines)
                  }
                }
              .map { ride =>
                var listUniqueLines = ride._4.toList
                var sortedUniqueLines = listUniqueLines.sortBy(_.timestamp.split(":")(1).toInt)
                var hour = ride._4.head.timestamp.split(":")(0)
                var date = ride._4.head.date
                var rideName = ride._4.head.ride_name
                var totalInterval = ride._1
                var totalWaiting = ride._2
                var rideID = ride._4.head.ride_id.toInt
                var currentWaitTime = sortedUniqueLines.last.wait_time.toFloat

                var meanWaitTime = (totalWaiting.toDouble / totalInterval)
                var midClamp = (5 * pow(currentWaitTime /max(1.toFloat, meanWaitTime), 1.25))
                var price = Math.clamp(midClamp, 2, 25).toFloat
                var priceRounded = (Math.round(price * 2) / 2.0).toFloat

                (rideName, rideID, currentWaitTime, meanWaitTime, hour, date, priceRounded)
              }
              .mergeSubstreams
              .fold(List.empty[(String, Int, Float, Double, String, String, Float)]) { (accumulator, ride) =>
                val updatedList = ride :: accumulator
                updatedList
              }
          }
          balance ~> fairPricesBuf ~> fairPricesThro ~> fairPrices ~> merge ~> toFlow

          FlowShape(balance.in, toFlow.out)
      }
    )

/***********************************************************************
Sink
***********************************************************************/

  // Creates a Sink that writes incoming ByteStrings to a specific file
  def getFileSink(filename: String): Sink[ByteString, Future[IOResult]] =
    FileIO.toPath(Paths.get(s"$resourcesFolder/results/$filename"), Set(CREATE, WRITE, APPEND))

  /***********************************************************************
Graphs Part
***********************************************************************/

  // Constructs the runnable graph
  val ladderGraph = RunnableGraph.fromGraph(
    GraphDSL.create(
      getFileSink("question1.txt"),
      getFileSink("question2.txt"),
      getFileSink("question3.txt"),
      getFileSink("question4.txt"),
      getFileSink("question5.txt"),
      getFileSink("question6.txt")
    )( (resultQ1, resultQ2, resultQ3, resultQ4, resultQ5, resultQ6) => Future.sequence(Seq(resultQ1, resultQ2, resultQ3, resultQ4, resultQ5, resultQ6)) ) { implicit builder =>
      (sinkQ1, sinkQ2, sinkQ3, sinkQ4, sinkQ5, sinkQ6) =>

        import GraphDSL.Implicits._

        val input = builder.add(source.via(csvParsing).via(mappingHeader).via(flowAttraction))

        val bcast1 = builder.add(Broadcast[RideQueueRecord](2))
        val bcast2 = builder.add(Broadcast[RideQueueRecord](2))
        val bcast3 = builder.add(Broadcast[RideQueueRecord](2))
        val bcast4 = builder.add(Broadcast[RideQueueRecord](2))
        val bcast5 = builder.add(Broadcast[RideQueueRecord](2))

        val question1 = builder.add(flowDifferentAttractions)
        val question2 = builder.add(flowTop5MeanTime)
        val question3 = builder.add(flowOpenInterval)
        val question4 = builder.add(flowLargestVariance)
        val question5 = builder.add(flowParkWaitTime)
        val question6 = builder.add(flowFairPrices)

        val sink1 = sinkQ1.asInstanceOf[SinkShape[ByteString]]
        val sink2 = sinkQ2.asInstanceOf[SinkShape[ByteString]]
        val sink3 = sinkQ3.asInstanceOf[SinkShape[ByteString]]
        val sink4 = sinkQ4.asInstanceOf[SinkShape[ByteString]]
        val sink5 = sinkQ5.asInstanceOf[SinkShape[ByteString]]
        val sink6 = sinkQ6.asInstanceOf[SinkShape[ByteString]]

        input ~> bcast1
                bcast1.out(0) ~> question1.in; question1.out ~> sink1.in
                bcast1.out(1) ~> bcast2

                                bcast2.out(0) ~> question2.in; question2.out ~> sink2.in
                                bcast2.out(1) ~> bcast3

                                                bcast3.out(0) ~> question3.in; question3.out ~> sink3.in
                                                bcast3.out(1) ~> bcast4

                                                                bcast4.out(0) ~> question4.in; question4.out ~> sink4.in
                                                                bcast4.out(1) ~> bcast5

                                                                                bcast5.out(0) ~> question5.in; question5.out ~> sink5.in
                                                                                bcast5.out(1) ~> question6.in; question6.out ~> sink6.in

        ClosedShape
    }
  )

  ladderGraph.run().onComplete {
    case Success(_) =>
      println("Finished")
      actorSystem.terminate()
    case Failure(e) =>
      println(s"Error: $e")
      actorSystem.terminate()
  }

