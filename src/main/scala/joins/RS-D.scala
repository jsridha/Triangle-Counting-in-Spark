package joins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RSDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\njoins.RSDMain <input dir> <output dir> <max value>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RS-D")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val MAX = args(2)

    val textFile = sc.textFile(args(0))
    val outgoings = textFile.map{ // Convert set of edges into data frame of outgoing edges from each user
        line => val Array(user1, user2) = line.split(",")
        (user1, user2)
    }.filter{ case (key, value ) => key.toInt < MAX && value.toInt < MAX } // Filter out edges where user1 and user2 are below MAX id value
      .toDF("User", "Out")
      
    val incomings = textFile.map{ // Convert set of edges into data frame of incoming edges to each user
        line => val Array(user1, user2) = line.split(",")
        (user2, user1) 
    }.filter{ case (key, value ) => key.toInt < MAX && value.toInt < MAX } // Filter out edges where user1 and user2 are below MAX id value
      .toDF("User", "In")

    val lastEdge = outgoings.join(incomings, "User") // Join outgoings with incomings on userID to generate list of two step paths.
    .filter($"Out" !== $"In") // Filter out two step loops
    .select($"Out".alias("Start"), $"In".alias("End")) // Return last edges to complete triangles.

    val triangles = lastEdge.join(outgoings, lastEdge("Start") === outgoings("User")) // Join lastEdge with outgoings to check what edges exists in outgoings.
      .filter($"End" === $"Out") // Filter to only keep lastEdges that exist in outgoings.
      .count()/3  // Count the number of triangles, dividing by three to remove duplicates.

    val resultDF = Seq("Number of triangles in the graph: " + triangles.toString).toDF("result")
    // Write the result to a text file
    resultDF.write.text(args(1))

    // println("Number of triangles in the graph: " + triangles)
  }
}