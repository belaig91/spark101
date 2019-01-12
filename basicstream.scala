##### sbt dependencies
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

#########################

import org.apache.spark._
import org.apache.spark.streaming._

object basicStream {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("basic stream").setMaster("local[2]")
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9000)
    val wordsFlatMap = lines.flatMap(_.split(" "))
    val wordsMap = wordsFlatMap.map(word => (word,1))
    val wordCount = wordsMap.reduceByKey(_+_)
    wordCount.print
    ssc.start()
    ssc.awaitTermination()
  }
}

