package com.jiyong

/**
  * 读取Neo4j中的数据进行查询
  */

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.neo4j.spark._

object SparkOpeaNeoj {
  def main(args: Array[String]): Unit = {
    // 创建spark对象
    val spark = SparkSession.builder().appName("SparkOpeaNeoj")
      .master("local[2]")
      .config("spark.neo4j.bolt.url", "bolt://10.12.64.250:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456")
      .getOrCreate()

    // 导入隐式转换包
    import spark.implicits._

    // 创建Neo4j对象
    val neo = Neo4j(spark.sparkContext)

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
    //*******************************************************************
    // 方式三：转化为Graph
  /*  val rawGarphNode = neo.cypher("MATCH (n:person)where (n.duration <>0) " +
      "RETURN n.user as user,n.other as other,n.direction as direction,n.duration as duration,n.timestamp as timestamp")
        .loadNodeRdds

    val neo4j1 = Neo4j(sc = spark.sparkContext).rels(relquery)
    val graph: Graph[Long, String] = neo4j1.loadGraph[Long,String]

    println(graph.vertices.count())

    rawGarphNode.take(10).foreach(println(_))
*/

  }
}
