name := "Streaming_test_demo"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.0",
  "org.apache.spark".%("spark-core_2.11").%("2.3.0"),
  "org.apache.hadoop".%("hadoop-client").%("2.8.4")
)