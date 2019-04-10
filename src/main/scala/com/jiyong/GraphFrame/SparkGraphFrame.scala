package com.jiyong.GraphFrame

/**
  * GraphFrames集成Neo4j测试
  * 从Neo4j中获取数据，利用GraphFrames进行查询
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame
import org.neo4j.spark.Neo4j

object SparkGraphFrame {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf，配置Neo4j信息
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
      .set("spark.neo4j.bolt.url", "bolt://10.12.64.244:7687")
      .set("spark.neo4j.bolt.user", "neo4j")
      .set("spark.neo4j.bolt.password", "123456")

    // 创建SparkContext
    val sc = new SparkContext(conf)

    // 创建Neo4j对象
    val neo = Neo4j(sc)

    // 本案例针对的Neo4j中存储的节点是BehaviorPage(appType,incomingCount,key,outcomingCount,phoneModel)
    // 拉取节点数据
    val rawGraphnode: RDD[Row] = neo.cypher("MATCH (n:BehaviorPage) RETURN id(n) as id," +
      "n.key as page").loadRowRdd

    // 将初始RDD转化为VertexRDD
    val pageRDD: RDD[(VertexId, String)] = rawGraphnode.map(row => {
      val id = row.getAs[Long]("id")
      val page = row.getAs[String]("page")
      (id,page)
    })

    // 拉取边的数据
    val rawGraphedge: RDD[Row] = neo.cypher("MATCH (n:BehaviorPage)-[r]->(m:BehaviorPage) " +
      "RETURN id(n) as source,id(m) as target,r.userId as userid").loadRowRdd

    // 将初始RDD转化为EdgeRDD
    val relationshipRDD = rawGraphedge.map(row => {
      val srcid = row.getAs[Long]("source")
      val dstid = row.getAs[Long]("target")
      val relationship = row.getAs[Long]("userid")
      Edge(srcid,dstid,relationship)
    })

    // 构建图模型
    val graph: Graph[String, VertexId] = Graph(pageRDD,relationshipRDD)

    val gf: GraphFrame = GraphFrame.fromGraphX(graph)
    val g: Graph[Row, Row] = gf.toGraphX
//    gf.vertices.show()
//    gf.edges.show()
//    gf.triplets.show()

    val count1 = gf.find("(a)-[e]->(b); (b)-[e2]->(c)")
      .filter("a.attr = 'Home' AND b.attr = 'CouponScan' AND c.attr = 'CustomerSuccess' AND e.attr = e2.attr")
      .count()

    println("从Home -> CouponScan -> CustomerSuccess 路径共有" + count1 + "人访问")

    val count2 = gf.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")
      .filter("a.attr = 'Home' AND b.attr = 'CouponScan' AND c.attr = 'CouponInput' AND d.attr = 'CustomerSuccess'" +
        "AND e.attr = e2.attr AND e2.attr = e3.attr")
      .count()

    println("从Home -> CouponScan -> CouponInput -> CustomerSuccess 路径共有" + count2 + "人访问")

  }

}
