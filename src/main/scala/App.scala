import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object App {
  def main(args: Array[String]) {
    val path = if (args.isEmpty) "data/*" else args.mkString(",")

    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("Remoting").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(path).filter(_.length > 0)
    val words = lines.flatMap(_.split(" "))
    
    stats(lines, "Lines")
    println()
    stats(words, "Words")
  }
  
  def stats(data: RDD[String], label: String) {
    val lengths = data.map(_.length)
    
    println(f"${label}: ${data.count}")
    println(f"  min length: ${lengths.min}")
    println(f"  mean length: ${lengths.mean}%.2f")
    println(f"  max length: ${lengths.max}")
  }
}
