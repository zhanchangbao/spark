package com.jiyong

import org.apache.spark.graphx.{GraphLoader, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPageRank {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc,"data/followers.txt")

    // Run PageRank
    val ranks: VertexRDD[Double] = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users: RDD[(Long, String)] = sc.textFile("data/users.txt").map(line => {
      val fields: Array[String] = line.split(",")
      (fields(0).toLong,fields(1))
    })
    val rankByUsername: RDD[(String, Double)] = users.join(ranks).map(t => {
      (t._2._1,t._2._2)
    })

    // print the result
    rankByUsername.foreach(println(_))

  }

}
