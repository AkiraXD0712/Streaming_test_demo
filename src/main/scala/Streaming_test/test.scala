package Streaming_test

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.BasicConfigurator
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.log4j.{Level, Logger}
import scala.io.Source

class HdfsClient {
  BasicConfigurator.configure()
  println( "Trying to connect to HDFS..." )
  private val fs = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    FileSystem.get(conf)
  }

  def mkDir (hdfsPath: String) = {
    try {
      fs.mkdirs(new Path(hdfsPath), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
    catch {
      case e: FileAlreadyExistsException => {
        e.printStackTrace()
      }
    }
  }

  def rmDir(hdfsPath: String) = {
    fs.delete(new Path(hdfsPath), true)
  }

  def write(hdfsPath: String, data: String, overwrite: Boolean = false) = {
    val path = new Path(hdfsPath)
    if (fs.exists(path) && overwrite) {
      fs.delete(path, true)
    }
    try {
      fs.create(path).write(data.getBytes)
    }
    catch {
      case e: FileAlreadyExistsException => {
        e.printStackTrace()
      }
    }
  }

  def writeStreaming(hdfsPath: String, filePath: String) = {
    for ((line, i) <- Source.fromFile("C:\\Users\\chen.lichong\\Desktop\\test.txt")
      .getLines()
      .map((x: String) => x.replaceAll(" ", ""))
      .zipWithIndex) {
      write(hdfsPath.concat(f"/$i%d.txt"), line)
    }
  }

  def close() = {
    fs.close()
  }
}

object Test{
  def main(args: Array[String]) {
    //    val hdfsClient = new HdfsClient()
    //    hdfsClient.write("/user/hadoop/test.txt", f"Hello World")
    //    hdfsClient.mkDir("/user/hadoop/testFolder")
    //    hdfsClient.writeStreaming("/user/hadoop/testFolder",
    //      "C:\\Users\\chen.lichong\\Desktop\\test.txt")
    //    hdfsClient.close()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming_test")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.checkpoint("/user/hadoop/checkpoint")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.textFileStream("hdfs://localhost:9000/user/hadoop/Streaming_test")
    val wordDstream = lines.flatMap(_.split(" ")).map(x => (x, 1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
