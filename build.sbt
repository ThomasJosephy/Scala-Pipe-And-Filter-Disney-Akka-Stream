import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "AkkaStreams"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.8.5",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "6.0.2"
)
