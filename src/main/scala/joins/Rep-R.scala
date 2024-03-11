package joins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RepRMain {

    def main(args: Array[String]) {
        val logger: org.apache.log4j.Logger = LogManager.getRootLogger
        if (args.length != 2) {
            logger.error("Usage:\njoins.RepRMain <input dir> <output dir>")
            System.exit(1)
        }
        val conf = new SparkConf().setAppName("Rep-R")
        val sc = new SparkContext(conf)

            // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
            // ================

        val MAX = 40000

        val textFile = sc.textFile(args(0))

        val edges = textFile.map(line => line.split(","))
            .filter(users => users(0).toInt < MAX && users(1).toInt < MAX) 
            .map(users => (users(0), users(1))) // Create a Pair RDD of all edges within the graph

        val H = edges.groupByKey() // Create a second Pair RDD that contains a map of users to the set of users they are following

        // Broadcast HashMap H 
        val broadcastH = sc.broadcast(H.collectAsMap())

        val triangles = edges.flatMap{ case(start, middle) => {
            val neighbors = broadcastH.value.getOrElse(middle, Iterable[String]()) // Get the neighbors of middle

            neighbors.flatMap { end =>
                val endNeighbors = broadcastH.value.getOrElse(end, Iterable[String]()) // Get the neighbors of end
                if (endNeighbors.exists(_ == start)) {
                    Some(1) // If there is a path from a neighbor of middle back to start, count 1 triangle
                } else {
                    None
                }
            }
        }}.count()/3 // Count the number of triangles, dividing by three to remove duplicates

        val resultRDD = sc.parallelize(Seq("Number of triangles in the graph: " + triangles.toString))
        resultRDD.saveAsTextFile(args(1))
        
        // println("Number of triangles in the graph: " + triangles)
    }
}