package com.jiyong.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.neo4j.spark.Neo4j

object SparkOpeaNeoj {
  def main(args: Array[String]): Unit = {
    test1()
    test2()
  }

  def test1() {
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
      (id, (name, Occupation))
    })

    //    userRDD.foreach(println(_))
    //    println("**********************************************")

    // 拉取边的数据
    // 此处只能用loadRowRdd,用loadRelRdd会报错，No relationship query provided either as pattern or with rels()
    val rawGraphedge: RDD[Row] = neo.cypher("MATCH (n:User)-[r]->(m:User) RETURN id(n) as source, " +
      "id(m) as target, type(r) as relationship").loadRowRdd

    val relationshipRDD: RDD[Edge[String]] = rawGraphedge.map(row => {
      val srcid = row.getAs[Long]("source")
      val dstid = row.getAs[Long]("target")
      val relationship = row.getAs[String]("relationship")
      Edge(srcid, dstid, relationship)
    })

    //    relationshipRDD.foreach(println(_))
    //    println("**********************************************")

    // Define a default user in case there are relationship with missing user
    val defaultUser: (String, String) = ("John Doe", "Missing")

    // Build the initial Graph,Graph的类型根据Vertex和Edge决定，Graph[(String, String), String]中
    // (String, String)代表Vertex属性类型，String代表Edge属性类型

    val graph: Graph[(String, String), String] = Graph(userRDD, relationshipRDD, defaultUser)

    // Count all users which are postdocs
    val vertices: VertexRDD[(String, String)] = graph.vertices

    val edges: EdgeRDD[String] = graph.edges

    //    graph.vertices.filter(t => {t._2._2 == "postdoc"}).foreach(println(_))
    //    println("**********************************************")

    // Count all the edges where src > dst
    //    graph.edges.filter(t => {t.dstId < t.srcId}).foreach(println(_))
    //    println("**********************************************")

    graph.degrees.foreach(println(_))
    vertices.foreach(println(_))
    println(edges.count())

    // Use the triplets view to create an RDD of facts.
    //    graph.triplets.map(t => {t.srcAttr._1 + " is the " + t.attr + " of " + t.dstAttr._1}).foreach(println(_))
    //    println("**********************************************")

    // 创建graphStream对象，对图数据分析结果进行可视化
    val graphStream = new SingleGraph("zcb")

    for ((id, t) <- graph.vertices.collect()) {
      val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
      // 加上顶点属性，可加可不加
      //      node.addAttribute("ui.label",id.toString + "\n" + "name:" + t._1 + "\n"+"age" + t._2)
      node.addAttribute("ui.label", id.toString)
      //      node.addAttribute("ui.label","name: " + t._1 + "\n"+"age" + t._2)
    }

    val ab: Array[Edge[String]] = graph.edges.collect()
    for (Edge(x, y, _) <- graph.edges.collect()) {
      val edge = graphStream.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
      // 加上边属性，可加可不加
      edge.addAttribute("ui.label", x.toString)
      edge.addAttribute("ui.label", y.toString)
    }

    // 通过这个样式文件来控制可视化的方式
    // ui.quality 和 ui.antialias 属性是告诉渲染引擎在渲染时以质量为先而非速度.
    // 如果不设置样式文件, 顶点与边默认渲染出来的效果是黑色.
    graphStream.addAttribute("ui.stylesheet", "url(file:src/resources/style/stylesheet.css)")
    graphStream.addAttribute("ui.quality")
    graphStream.addAttribute("ui.antialias")

    graphStream.display()

  }

  def test2(): Unit ={
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

    /* 本例中Neo4j中的节点是一个person的实例，在转化为rdd的时候，希望rdd其实wrapper的是person class ，可以创建一个case class
    case class Person(user: String,other:String,direction:String,duration:String,timestamp:String)
    也可以直接用AS语法*/

    // ****************************************************************
    //方式一：转化为RDD
    // neo.cyper创建的都是Neo4j对象，可以通过不同的方法比如loadRDD,loadDataFrame,loadGraph转换为Spark操作对象
    val rawGarphNode: RDD[Row] = neo.cypher("MATCH (n:person)where (n.duration <>0) " +
      "RETURN n.user as user,n.other as other,n.direction as direction,n.duration as duration,n.timestamp as timestamp")
      .loadRowRdd

    // 针对RDD定义算子操作
    rawGarphNode.take(10).foreach(println(_))

    // *****************************************************************
    //方式二：转化为DataFrame(使用AS语法)
    // 想直接转DataFrame 其实还是有阻碍的,单个属性可以，多个属性就报废了，schema的语法是(fieldName,fieldtype),
    // 其中type 支持 String double long boolean ,如果是object integer float，会自动转化
    //另外需要在查询语句中使用as语法，这样就转化成功了,唯一的缺点就是structType 都是string
    /*val rawGarphNode: DataFrame = neo.cypher("MATCH (n:person)where (n.duration <>0) " +
      "RETURN n.user as user,n.other as other,n.direction as direction,n.duration as duration,n.timestamp as timestamp")
      .loadDataFrame(schema = ("user","object"),("other","object"),("direction","string"),("duration","String"),("timestamp","String"))

    rawGarphNode.printSchema()
    rawGarphNode.show(10)*/
    // DataFrame有schema信息，而RDD是没有的
    //*****************************************************************
    // 方式三：转化为Graph
    /*al rawGarphNode = neo.cypher("MATCH (n:person)where (n.duration <>0) " +
        "RETURN n.user as user,n.other as other,n.direction as direction,n.duration as duration,n.timestamp as timestamp")
          .loadNodeRdds

      val neo4j1 = Neo4j(sc = spark.sparkContext).rels(relquery)
      val graph: Graph[Long, String] = neo4j1.loadGraph[Long,String]

      println(graph.vertices.count())

      rawGarphNode.take(10).foreach(println(_))*/

  }

}
