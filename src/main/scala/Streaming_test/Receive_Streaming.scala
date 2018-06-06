package Streaming_test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.log4j.{Level, Logger}

object Receive_Streaming{
  def main(args: Array[String]) {
    // Set the Log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Initialisation
    println("Initialize Spark..........")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming_test")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("/user/hadoop/checkpoint")
    // Stage
    // val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.socketTextStream("localhost", 9999)
    // val lines = ssc.textFileStream("hdfs://localhost:9000/user/hadoop/Streaming_test")
    val logDS = lines.map(_.split(" "))
      .filter(_(9) == "-")
      .map((l: Array[String]) => Array(l(0), l(1), l(9)).mkString(";"))
    // val wordDstream = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

//    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
//      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
//      val output = (word, sum)
//      state.update(sum)
//      output
//    }
//
//    val stateDstream = wordDstream.mapWithState(
//      StateSpec.function(mappingFunc).initialState(initialRDD))
//
//    stateDstream.print()
    logDS.print()
    println("Start..........")
    ssc.start()
    ssc.awaitTermination()
  }
}