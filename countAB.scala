## sbt dependencies

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

################

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object countAB {
  def main(args: Array[String]): Unit ={
    val shak = "/home/belay/shakespeare.txt"
    val conf = new SparkConf().setAppName("simple app").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val shaks = sc.textFile(shak, 2).cache()
    val numAs = shaks.filter(line => line.contains("a")).count()
    val numBs = shaks.filter(line =>line.contains("b")).count()
    println("lines with a: %s, line with b: %s".format(numAs,numBs))

  }
}
