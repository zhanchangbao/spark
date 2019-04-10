package com.jiyong.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphxExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    // Create an RDD for the vertices
    /**
      * 导入import org.apache.spark.graphx.VertexId之后Long类型的会变成VertexId
      * 不导的话默认的是Long类型
      * val users: (Long, (String, String)) = Tuple2(3L,("rxin", "student"))
      * val users: (Long, (String, String)) = (3L,("rxin", "student"))
      * val users: (String, String) = ("rxin", "student")
      */

    val userList: Array[(VertexId, (String, String))]
    = Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))
    )
    val userRDD: RDD[(VertexId, (String, String))]
    = sc.parallelize(userList)
    val VD: VertexRDD[(String, String)]
    = VertexRDD(userRDD)

    // Create an RDD for edges
    /**
      * 用到了Edge样本类。边有一个srcId和dstId分别对应于源和目标顶点的标示符。另外，
      * Edge类有一个attr成员用来存储边属性,Edge的类型是它的属性类型，不包括srcid和dstid类型。
      * 我们可以分别用graph.vertices和graph.edges成员将一个图解构为相应的顶点和边。
      */

    val relationshipList: Array[Edge[String]]
    = Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")
    )


    val relationshipRDD = sc.parallelize(relationshipList)


    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String)
    = ("John Doe", "Missing")

    // Build the initial Graph,Graph的类型根据Vertex和Edge决定，Graph[(String, String), String]中
    // (String, String)代表Vertex属性类型，String代表Edge属性类型
    val graph: Graph[(String, String), String]
    = Graph(userRDD, relationshipRDD, defaultUser)

    // ************************************************************
    // 上面根据VertexRDD和EdgeRDD创建了graph，可以利用graph的各种方法

    /**
      * 获取graph中的属性，如vertices、edges、triplets、degrees、inDegrees、outDegrees
      * graph返回的都是RDD类型，不同的属性返回的属性类型不同而已，可以针对这些属性使用RDD的算子进行操作
      * 也可以利用graph的方法进行操作，Spark的每一个模块都有一个核心抽象，这些核心抽象都是基于RDD的，
      * 可以基于模块本身的抽象进行操作，也可以基于最底层的RDD抽象进行操作
      *
      */

    val vertices: VertexRDD[(String, String)]
    = graph.vertices
    val edges: EdgeRDD[String]
    = graph.edges
    val triplets: RDD[EdgeTriplet[(String, String), String]]
    = graph.triplets
    val degrees: VertexRDD[PartitionID]
    = graph.degrees
    val inDegrees: VertexRDD[PartitionID]
    = graph.inDegrees
    val outDegrees: VertexRDD[PartitionID]
    = graph.outDegrees
    val numEdges: VertexId
    = graph.numEdges
    val numVertices: VertexId
    = graph.numVertices

    /**
      * graph.vertices返回一个VertexRDD[(String, String)]，它继承于 RDD[(VertexID, (String, String))]。
      * 可以以用scala的case表达式解构这个元组。另一方面，
      * graph.edges返回一个包含Edge[String]对象的EdgeRDD。我们也可以用到case类的类型构造器
      * 也可以直接用Tuple取值解析
      *
      */
    // Count all users which are postdocs
    vertices.filter(t => {
      t._2._2 == "postdoc"
    })

    // Count all the edges where src > dst
    edges.filter(t => {
      t.dstId > t.dstId
    })

    // Use the triplets view to create an RDD of facts.
    triplets.map(t => {
      t.srcAttr._1 + " is the " + t.attr + " of " + t.dstAttr._1
    }).foreach(println(_))
    println("****************************************************************************************")

    /**
      * subgraph⇒Boolean,(VertexId,VD)⇒Boolean):Graph[VD,ED])操作利用顶点和边的谓词（predicates），
      * 返回的图仅仅包含满足顶点谓词的顶点、满足边谓词的边以及满足顶点谓词的连接顶点（connect vertices）。
      * subgraph操作可以用于很多场景，如获取 感兴趣的顶点和边组成的图或者获取清除断开链接后的图。有点类似于过滤
      * 下面的例子删除了断开的链接。
      */

    // Remove missing vertices as well as the edges to connected to them
    val validGraph: Graph[(String, String), String]
    = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect().foreach(println(_))
    validGraph.triplets.map(t => {
      t.srcAttr._1 + " is the " + t.attr + " of " + t.dstAttr._1
    }).foreach(println(_))

    val ccGraph = graph.connectedComponents() // No longer contains missing field

    val validCCGraph = ccGraph.mask(validGraph)
  }

}
