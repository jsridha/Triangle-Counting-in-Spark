package joins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RSRMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\njoins.RSRMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RS-R")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val MAX = 30000

    val textFile = sc.textFile(args(0))
    val outgoings = textFile.map{ // Create a Pair RDD of outgoing edges
        line => val Array(user1, user2) = line.split(",")
        (user1, user2)
    }.filter{ case (key, value) => key.toInt < MAX && value.toInt < MAX } // Filter out edges where user1 and user2 are below MAX id value

    val incomings = textFile.map{ // Create a Pair RDD of incoming edges
        line => val Array(user1, user2) = line.split(",")
        (user2, user1) 
    }.filter{ case (key, value) => key.toInt < MAX && value.toInt < MAX } // Filter out edges where user1 and user2 are below MAX id value

    val lastedge = outgoings.join(incomings) // Join incoming and outgoing RDDs to return sets of last edges to complete triangles
      .filter{ case (_, (node1, node2)) => node1 != node2 } // Remove two step loops
      .flatMap { case (_, (node1, node2)) => Seq((node1, node2)) } // Flatten pair RDD to remove keys based on midpoint

    val triangles = lastedge.join(outgoings)
      .filter { case (_, (node1, node2)) => node1 == node2 } // Filter to find what last legs exist in the set of edges
      .count()/3 // Count the number of triangles, dividing by three to remove duplicates

    val resultRDD = sc.parallelize(Seq("Number of triangles in the graph: " + triangles.toString))
    resultRDD.saveAsTextFile(args(1))

    // println("Number of triangles in the graph: " + triangles)
  }
}