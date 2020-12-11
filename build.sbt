name := "spark"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-avro" % "2.4.0"

)

