package com.jiyong.graphx

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 1、求最大度数
  * 2、取出前5个节点
  * 3、pageRank算法找出最重要的节点
  * 4、mapTriplets的用法，对图的边属性进行增加，与mapEdges区别为同时对三元组进行转换
  * 5、mapEdges操作，只能对边的属性进行操作（不能拿到对象的相关信息）
  * 6、mapVertices操作，只能对节点进行操作
  * 7、使用aggregateMessages操作，计算每个节点的出度（join）
  * 8、使用aggregateMessages操作，计算每个节点的入度（rightOuterJoin）
  * 9、使用aggregateMessages计算每个节点与根节点的距离
  * 10、使用Pregel计算每个节点与根节点的距离
  * 11、官网的aggregateMessages例子
  * 12、Spark Graphx图计算案例实战之aggregateMessages求社交网络中的最大年纪追求者和平均年纪！
  * 13、生成Gexf的xml文件
  */


/**
  * 求最大度数
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "E:\\测试文本\\cit-HepTh\\Cit-HepTh.txt")
    val degreeRdd = graph.inDegrees
    val tuple: (VertexId, Int) = degreeRdd.reduce((a, b) => if (a._2 > b._2) a else b)

    println(tuple)
    // res: (9711200,2414)

  }
}

/**
  * 取出前5个节点
  */
object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "E:\\测试文本\\cit-HepTh\\Cit-HepTh.txt")
    println(graph.vertices.take(5).toBuffer)
  }
}

/**
  * pageRank算法找出最重要的节点
  */
object Test4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "E:\\测试文本\\cit-HepTh\\Cit-HepTh.txt")
    val res: Graph[Double, Double] = graph.pageRank(0.001)
    //    val vertices: Array[(VertexId, Double)] = res.vertices.take(5)
    println(res.vertices.reduce((a, b) => if (a._2 > b._2) a else b))

  }
}

/**
  * mapTriplets的用法，对图的边属性进行增加，与mapEdges区别为同时对三元组进行转换
  */
object MapTriplets {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val trip = myGraph.mapTriplets(t => {
      (t.attr, t.attr == "is-friends-with" && t.srcAttr.toLowerCase.contains("a"))
    })

    val triplets: RDD[EdgeTriplet[String, (String, Boolean)]] = trip.triplets
    println(triplets.collect().toBuffer)
    //    ArrayBuffer(((1,Ann),(2,Bill),(is-friends-with,true)), ((2,Bill),(3,Charles),(is-friends-with,false)), ((3,Charles),(4,Diane),(is-friends-with,true)), ((4,Diane),(5,Went to gym this morning),(Likes-status,false)), ((3,Charles),(5,Went to gym this morning),(Wrote- status,false)))

  }
}

/**
  * mapEdges操作，只能对边的属性进行操作（不能拿到对象的相关信息）
  */

object MapEdgesTest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MapEdges")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val myVertices: RDD[(VertexId, String)] = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    /*   val ts: RDD[(VertexId, String)] = sc.parallelize(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
         (5L, "Went to gym this morning")))
       */

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices,myEdges)
    Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status"))

    val graph1 = myGraph.mapEdges(e => {
      (e.attr, e.attr == "is-friends-with" && e.srcId == 1)
    })

    val triplets: RDD[EdgeTriplet[String, (String, Boolean)]] = graph1.triplets
    println(triplets.collect().toBuffer)
    //    ArrayBuffer(((1,Ann),(2,Bill),(is-friends-with,true)), ((2,Bill),(3,Charles),(is-friends-with,false)), ((3,Charles),(4,Diane),(is-friends-with,false)), ((4,Diane),(5,Went to gym this morning),(Likes-status,false)), ((3,Charles),(5,Went to gym this morning),(Wrote- status,false)))

  }
}
object MapEdges {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val graph1 = myGraph.mapEdges(e => {
      (e.attr, e.attr == "is-friends-with" && e.srcId == 1)
    })

    val triplets: RDD[EdgeTriplet[String, (String, Boolean)]] = graph1.triplets
    println(triplets.collect().toBuffer)
    //    ArrayBuffer(((1,Ann),(2,Bill),(is-friends-with,true)), ((2,Bill),(3,Charles),(is-friends-with,false)), ((3,Charles),(4,Diane),(is-friends-with,false)), ((4,Diane),(5,Went to gym this morning),(Likes-status,false)), ((3,Charles),(5,Went to gym this morning),(Wrote- status,false)))

  }
}

/**
  * mapVertices操作，只能对节点进行操作
  */
object MapVertices {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val graph1 = myGraph.mapVertices((vId: VertexId, attr: String) => {
      (attr, vId == 1)
    })

    val triplets: RDD[EdgeTriplet[(String, Boolean), String]] = graph1.triplets
    println(triplets.collect().toBuffer)
  }
}

/**
  * 使用aggregateMessages操作，计算每个节点的出度（join）
  */
object AggregateMessages {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val vertices1: VertexRDD[Int] = myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _)

    val vertice2: RDD[(VertexId, (Int, String))] = vertices1.join(myGraph.vertices)

    val vertice3: RDD[(String, Int)] = vertice2.map(_._2.swap)

    println(vertice3.collect().toBuffer)
    //    ArrayBuffer((Ann,1), (Bill,1), (Charles,2), (Diane,1))

  }
}

/**
  * 使用aggregateMessages操作，计算每个节点的入度（rightOuterJoin）
  */
object AggregateMessages2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val vertices1: VertexRDD[Int] = myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _)

    val vertice2: RDD[(VertexId, (Option[Int], String))] = vertices1.rightOuterJoin(myGraph.vertices).cache()

    val vertice3: RDD[(String, Option[Int])] = vertice2.map(_._2.swap)

    val vertice4: RDD[(String, PartitionID)] = vertice2.map(x => (x._2._2, x._2._1.getOrElse(0)))

    println(vertice3.collect().toBuffer)
    //    ArrayBuffer((Ann,Some(1)), (Bill,Some(1)), (Charles,Some(2)), (Diane,Some(1)), (Went to gym this morning,None))


    println(vertice4.collect().toBuffer)
    //    ArrayBuffer((Ann,1), (Bill,1), (Charles,2), (Diane,1), (Went to gym this morning,0))

  }
}

/**
  * 使用aggregateMessages计算每个节点与根节点的距离
  */
object AggregateMessages3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)
    val initGraph: Graph[Int, String] = myGraph.mapVertices((vId, attr) => {
      0
    })
    val graphRes: Graph[PartitionID, String] = proopagateEdgeCount(initGraph)
    val buffer: mutable.Buffer[(VertexId, PartitionID)] = graphRes.vertices.collect.toBuffer

    println(buffer)
    //    ArrayBuffer((1,0), (2,1), (3,2), (4,3), (5,4))
  }

  def sendMsg(ec: EdgeContext[Int, String, Int]) = {
    ec.sendToDst(ec.srcAttr + 1)
  }

  def mergeMsg(a: Int, b: Int): Int = {
    math.max(a, b)
  }

  def proopagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
    println("vertice--" + g.vertices.collect.toBuffer)

    val vertice1: VertexRDD[Int] = g.aggregateMessages[Int](sendMsg, mergeMsg)

    println("vertice1--" + vertice1.collect.toBuffer)

    val g2: Graph[Int, String] = Graph(vertice1, g.edges).cache()

    val vertice3: RDD[(VertexId, (Int, Int))] = g2.vertices.join(g.vertices)

    println("vertice3--" + vertice3.collect.toBuffer)

    val vertice4: RDD[Int] = vertice3.map(x => x._2._1 - x._2._2)

    val i: PartitionID = vertice4.reduce(_ + _)

    if (i > 0) {
      proopagateEdgeCount(g2)
    } else {
      g
    }

  }
}

/**
  * 使用Pregel计算每个节点与根节点的距离
  */
object PregelTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"),
      (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"), Edge(2L, 3L, "is-friends-with"),
      Edge(3L, 4L, "is-friends-with"), Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote- status")))

    val myGraph = Graph(myVertices, myEdges)

    val value: Graph[PartitionID, String] = myGraph.mapVertices((vId, attr) => 0)

    val g1 : Graph[Int,String] = (Pregel(value, 0, activeDirection = EdgeDirection.Out) // --注意最外层必须要有（）
      (
        (id: VertexId, vd: Int, a: Int) => math.max(vd, a),
        (et: EdgeTriplet[Int, String]) => Iterator((et.dstId, et.srcAttr + 1)),
        (a: Int, b: Int) => math.max(a, b)
      ))

    println(g1.vertices.collect.toBuffer)
    //    ArrayBuffer((1,0), (2,1), (3,2), (4,3), (5,4))

  }
}

/**
  * 官网的aggregateMessages例子
  */
object AggregateMessages4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)


    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) =>
        value match {
          case (count, totalAge) => totalAge / count
        })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }
}

/**
  * Spark Graphx图计算案例实战之aggregateMessages求社交网络中的最大年纪追求者和平均年纪！
  */
object AggregateMessages5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tempVertice = sc.textFile("E:\\测试文本\\users.txt").map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0).toLong, arr(1).toDouble)
    })

    val graph1: Graph[Int, Int] = GraphLoader.edgeListFile(sc,"E:\\测试文本\\followers.txt").cache()

    //重要方法，对已有的图进行顶点属性替换操作
    val graph2 = graph1.outerJoinVertices(tempVertice)(
      (vId,attr,option) => {
        option.getOrElse(0.0)
      } )

    val graph3 = graph1.joinVertices(tempVertice)((vId,attr,u) => {
      attr
    })

    // 进行聚合求出每个节点追随者的个数和总追随者总的年龄
    val vertice2 : VertexRDD[(Int, Double)]= graph2.aggregateMessages[(Int, Double)](
      ec => ec.sendToDst((1,ec.srcAttr)),
      (a,b) => (a._1 + b._1, a._2 + b._2)
    )

    // 对年龄求平均值
    val aveAgeRDD : VertexRDD[Double]= vertice2.mapValues((vId,tuple) => tuple._2/tuple._1)

    println(aveAgeRDD.collect.toBuffer)
    //    ArrayBuffer((4,37.0), (6,29.0), (2,23.0), (1,32.5), (3,33.0), (7,29.5))

    // 进行聚合求出年龄最大的追随者
    val maxAgeRDD = graph2.aggregateMessages[(VertexId,Double)](
      ec => ec.sendToDst((ec.srcId,ec.srcAttr)),
      (a,b) => if (a._2 > b._2) a else b
    )

    println("max age--" + maxAgeRDD.collect.toBuffer)
    //    max age--ArrayBuffer((4,(5,37.0)), (6,(7,29.0)), (2,(1,23.0)), (1,(2,34.0)), (3,(5,37.0)), (7,(6,33.0)))

    //    val pw=new java.io.PrintWriter("E:\\测试文本\\myGraph.gexf")
    //    pw.write(ToGexf.toGexf(graph2))
    //    pw.close


    /*println("tempVertice--" + tempVertice.collect().toBuffer)
//    tempVertice--ArrayBuffer((1,23.0), (2,34.0), (3,26.0), (4,31.0), (6,33.0), (7,29.0), (5,37.0))

    println("graph1--" + graph1.vertices.collect().toBuffer)
//    graph1--ArrayBuffer((4,1), (6,1), (2,1), (1,1), (3,1), (7,1), (5,1))

    println("graph2--" + graph2.vertices.collect().toBuffer)
//    graph2--ArrayBuffer((4,31.0), (6,33.0), (2,34.0), (1,23.0), (3,26.0), (7,29.0), (5,37.0))

    println("graph3--" + graph3.vertices.collect().toBuffer)
//    graph3--ArrayBuffer((4,1), (6,1), (2,1), (1,1), (3,1), (7,1), (5,1))*/
  }
}

/**
  * 生成Gexf的xml文件
  */
object ToGexf {
  def toGexf[VD,ED](g:Graph[VD,ED]) : String = {
    val string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      " <nodes>\n" +
      g.vertices.map(v => " <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      " </nodes>\n" +
      " <edges>\n" +
      g.edges.map(e => " <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      " </edges>\n" +
      " </graph>\n" +
      "</gexf>"
    return string
  }
}
