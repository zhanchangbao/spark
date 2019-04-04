package com.jiyong

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j
import org.apache.spark.graphx._
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object SparkReadNeo4j {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf，配置Neo4j信息
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
      .set("spark.neo4j.bolt.url", "bolt://10.12.64.250:7687")
      .set("spark.neo4j.bolt.user", "neo4j")
      .set("spark.neo4j.bolt.password", "123456")

    // 创建SparkContext
    val sc = new SparkContext(conf)

    // 创建Neo4j对象
    val neo = Neo4j(sc)

    // 本案例针对的Neo4j中存储的节点是User(name,Occupation)
    // 拉取节点数据
    // 可以用loadRowRdd也可以用loadNodeRdds
    val rawGraphnode: RDD[Row] = neo.cypher("MATCH (n:User) RETURN id(n)as id,n.name as name,n.Occupation as Occupation").loadRowRdd

    // 将初始RDD转化为VertexRDD
    val userRDD: RDD[(VertexId, (String, String))] = rawGraphnode.map(row => {
      val id = row.getAs[Long]("id")
      val name = row.getAs[String]("name")
      val Occupation = row.getAs[String]("Occupation")
      (id,(name,Occupation))
    })

    userRDD.foreach(println(_))
    println("**********************************************")

    // 拉取边的数据
    // 此处只能用loadRowRdd,用loadRelRdd会报错，No relationship query provided either as pattern or with rels()
  val rawGraphedge: RDD[Row] = neo.cypher("MATCH (n:User)-[r]->(m:User) RETURN id(n) as source, " +
      "id(m) as target, type(r) as relationship").loadRowRdd

    val relationshipRDD: RDD[Edge[String]] = rawGraphedge.map(row => {
      val srcid = row.getAs[Long]("source")
      val dstid = row.getAs[Long]("target")
      val relationship = row.getAs[String]("relationship")
      Edge(srcid,dstid,relationship)
    })

    relationshipRDD.foreach(println(_))
    println("**********************************************")

    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String) = ("John Doe", "Missing")

    // Build the initial Graph,Graph的类型根据Vertex和Edge决定，Graph[(String, String), String]中
    // (String, String)代表Vertex属性类型，String代表Edge属性类型

    val graph: Graph[(String, String), String] = Graph(userRDD,relationshipRDD,defaultUser)

    // Count all users which are postdocs
    val a: VertexRDD[(String, String)] = graph.vertices

    val b: EdgeRDD[String] = graph.edges

    graph.vertices.filter(t => {t._2._2 == "postdoc"}).foreach(println(_))
    println("**********************************************")

    // Count all the edges where src > dst
    graph.edges.filter(t => {t.dstId < t.srcId}).foreach(println(_))
    println("**********************************************")

    // Use the triplets view to create an RDD of facts.
    graph.triplets.map(t => {t.srcAttr._1 + " is the " + t.attr + " of " + t.dstAttr._1}).foreach(println(_))
    println("**********************************************")

    // 创建graphStream对象，对图数据分析结果进行可视化
    val graphStream = new SingleGraph("zcb")

    for((id,t) <- graph.vertices.collect()){
      val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
      // 加上顶点属性，可加可不加
      node.addAttribute("ui.label",id.toString + "\n" + "name:" + t._1 + "\n"+"age" + t._2)
    }

    val ab: Array[Edge[String]] = graph.edges.collect()
    for (Edge(x,y,_) <- graph.edges.collect()){
      val edge =  graphStream.addEdge(x.toString ++ y.toString,x.toString,y.toString,true).asInstanceOf[AbstractEdge]
      // 加上边属性，可加可不加
      edge.addAttribute("ui.label",x.toString)
    }

    graphStream.display()

  }

}


