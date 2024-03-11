package joins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RepDMain {

    def main(args: Array[String]) {
        val logger: org.apache.log4j.Logger = LogManager.getRootLogger
        if (args.length != 2) {
            logger.error("Usage:\njoins.RepRMain <input dir> <output dir>")
            System.exit(1)
        }
        val conf = new SparkConf().setAppName("Rep-R")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
        import spark.implicits._

            // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
            // ================

        val MAX = 1000

        // Read input text file into a RDD
        val textFile = sc.textFile(args(0))

        // Convert textFile to a DataFrame of edges
        val edges = textFile.map(line => line.split(","))
            .filter(users => users(0).toInt < MAX && users(1).toInt < MAX) 
            .map(users => (users(0), users(1)))
            .toDF("Start", "End")

        val edgesRenamed = edges.select($"Start".as("df1_start"), $"End".as("df1_end"))

        // Broadcast the edges DataFrame
        val edgesBroadcast = sc.broadcast(edges.select($"Start".as("df2_start"), $"End".as("df2_end")))

        // Find the last edges that would complete a triangle
        val lastEdge = edgesRenamed.as("StartToMiddle")
            .join(edgesBroadcast.value.as("MiddleToEnd"), 
                    ($"StartToMiddle.df1_end" === $"MiddleToEnd.df2_start") && 
                    ($"StartToMiddle.df1_start" !== $"MiddleToEnd.df2_end"))
            .select($"df2_end".as("df1_start"),$"df1_start".as("df1_end"))

        // Find count of last edges that exist in the broadcast dataframe. Divide by three to count the number of triangles
        val triangles = lastEdge.as("LastEdge")
            .join(edgesBroadcast.value.as("EndToStart"),
                ($"LastEdge.df1_start" === $"EndToStart.df2_start") && 
                ($"LastEdge.df1_end" === $"EndToStart.df2_end"))
            .count() / 3

        // Save result to output directory
        val resultDF = Seq("Number of triangles in the graph: " + triangles.toString).toDF("result")
        resultDF.write.text(args(1))
    }
}