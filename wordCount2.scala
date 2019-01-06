
// first thing first we we import dependencies
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordCount2 {
  def main(args: Array[String]): Unit = {
    val inputFile = args(0) // input file path
    val outputFile = args(1) //output file path
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount2")
    val sc = new SparkContext(conf) // intialize sparkcontext
    val input = sc.textFile(inputFile) //load file and creating RDD
    val words = input.flatMap(_.split(" ")) //split into words
    val counts = words.map(word => (word, 1)).reduceByKey(_+_) // map words and aggregate
    counts.saveAsTextFile(outputFile) // saving into the path

  }
}
