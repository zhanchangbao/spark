package com.jiyong

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object SparkWithGraph {

  def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setAppName("GraphStreamDemo")
          .setMaster("local[2]")

        val sc = new SparkContext(conf)

        val graph = new SingleGraph("zcb")

        //设置定点和边，定点和边都是用元组定义的Array
        //定点的数据类型是VD:(String,Int)
        val vertexArray = Array(
          (1L,("Alice",28)),
          (2L,("Bob",27)),
          (3L,("Charlie",65)),
          (4L,("David",42)),
          (5L,("Ed",55)),
          (6L,("Fran",50))
        )

        //边的数据类型为ED:Int
        val edgeArray = Array(
          Edge(2L, 1L, 7),
          Edge(2L, 4L, 2),
          Edge(3L, 2L, 4),
          Edge(3L, 6L, 3),
          Edge(4L, 1L, 1),
          Edge(5L, 2L, 2),
          Edge(5L, 3L, 8),
          Edge(5L, 6L, 3)
        )

        //构造verteRDD和edgeRDD
        val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
        val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        //构造图Graph[VD,ED]
        val srcGraph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRDD)
        val s: Array[(VertexId, (String, Int))] = srcGraph.vertices.collect()
        val a = s(0)._2

        for((id,t) <- srcGraph.vertices.collect()){
          val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
          // 加上顶点属性，可加可不加
          node.addAttribute("ui.label",id.toString + "\n" + "name:" + t._1 + "\n"+"age" + t._2)
        }

        val ab: Array[Edge[Int]] = srcGraph.edges.collect()
        for (Edge(x,y,_) <- srcGraph.edges.collect()){
          val edge =  graph.addEdge(x.toString ++ y.toString,x.toString,y.toString,true).asInstanceOf[AbstractEdge]
          // 加上边属性，可加可不加
          edge.addAttribute("ui.label",x.toString)
        }

        graph.display()

      }

}
