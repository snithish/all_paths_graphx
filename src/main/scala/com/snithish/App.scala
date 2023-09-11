package com.snithish

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{mutable, Map}
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object App {
  type Paths = Set[List[String]]
  type Message = Map[VertexId, Paths]

  case class VertexAttr(name: String, isSource: Boolean, pathMap: Message)

  def randomGraph(spark: SparkSession, nVertex: Int): Graph[String, Long] = {
    val graph: Graph[VertexId, PartitionID] = GraphGenerators.logNormalGraph(spark.sparkContext, nVertex)
    graph.mapVertices((x, _) => f"vertex_$x%s").mapEdges(e => e.attr.toLong)
  }

  def buildGraph(spark: SparkSession): Graph[String, Long] = {
    val nodes =
      spark.sparkContext.parallelize((1 to 17).toList.map(x => (x.longValue, f"vertex_$x%s")))
    // Create an RDD for edges
    val edges: RDD[Edge[Long]] =
      spark.sparkContext.parallelize(Seq(Edge(2L, 8L, 1L),
        Edge(1L, 1L, 1),
        Edge(3L, 8L, 1),
        Edge(4L, 9L, 1),
        Edge(5L, 9L, 1),
        Edge(6L, 10L, 1),
        Edge(6L, 11L, 1),
        Edge(7L, 11L, 1),
        Edge(8L, 9L, 1),
        Edge(11L, 10L, 1),
        Edge(9L, 10L, 1),
        Edge(9L, 2L, 1),
        //        Edge(10L, 2L, 1),
        Edge(10L, 12L, 1),
        Edge(12L, 13L, 1),
        Edge(13L, 14L, 1),
        Edge(14L, 15L, 1),
      ))
    // Build the initial Graph
    Graph(nodes, edges)
  }

  def cycleGraph(spark: SparkSession): Graph[String, Long] = {
    val nodes =
      spark.sparkContext.parallelize((1 to 6).toList.map(x => (x.longValue, f"vertex_$x%s")))
    val edges: RDD[Edge[Long]] =
      spark.sparkContext.parallelize(Seq(Edge(1L, 2L, 1L),
        Edge(2L, 3L, 1),
        Edge(3L, 4L, 1),
        Edge(4L, 5L, 1),
        Edge(5L, 6L, 1),
        Edge(6L, 1L, 1),
      ))
    // Build the initial Graph
    Graph(nodes, edges)
  }

  def cycleGraphOneExit(spark: SparkSession): Graph[String, Long] = {
    val nodes =
      spark.sparkContext.parallelize((1 to 7).toList.map(x => (x.longValue, f"vertex_$x%s")))
    val edges: RDD[Edge[Long]] =
      spark.sparkContext.parallelize(Seq(Edge(1L, 2L, 1L),
        Edge(2L, 3L, 1),
        Edge(3L, 4L, 1),
        Edge(4L, 5L, 1),
        Edge(5L, 6L, 1),
        Edge(6L, 1L, 1),
        Edge(6L, 7L, 1),
      ))
    // Build the initial Graph
    Graph(nodes, edges)
  }

  def dag(spark: SparkSession): Graph[String, Long] = {
    /*
    1
   / \
  2   3
 / \   \
4   5   |
/ \   \ |
6   7   8
\
9
     */
    val nodes =
      spark.sparkContext.parallelize((1 to 9).toList.map(x => (x.longValue, f"vertex_$x%s")))
    val edges: RDD[Edge[Long]]
    = spark.sparkContext.parallelize(
      Seq(
        Edge(1L, 2L, 1),
        Edge(1L, 3L, 1),
        Edge(2L, 4L, 1),
        Edge(2L, 5L, 1),
        Edge(3L, 8L, 1),
        Edge(4L, 6L, 1),
        Edge(4L, 7L, 1),
        Edge(5L, 8L, 1),
        Edge(6L, 9L, 1)
      )
    )
    Graph(nodes, edges)
  }

  /*
  initialMsg: Message,
  maxIterations: Int = Int.MaxValue,
  activeDirection: EdgeDirection = EdgeDirection.Out)(
  vprog: (VertexId, VertexAttr, Message) => VertexAttr,
  sendMsg: EdgeTriplet[VertexAttr, Long] => Iterator[(VertexId, Message)],
  mergeMsg: (Message, Message) => Message
   */

  val initMsg: Message = Map.empty[VertexId, Paths]
  val emptyPath = Set.empty[List[String]]

  def vProg(vid: VertexId, attr: VertexAttr, incomingMsg: Message): VertexAttr = {
    // Need to combine here based on the vertex id from which message is received, since not all vertex will send message
    val existingState = attr.pathMap
    val allKeys = existingState.keySet ++ incomingMsg.keySet
    val newPathMap = mutable.Map[VertexId, Paths]()
    allKeys.foreach(x => {
      val updatedPath = incomingMsg.getOrElse(x, existingState.getOrElse(x, emptyPath))
      newPathMap.put(x, updatedPath)
    })
    VertexAttr(attr.name, attr.isSource, newPathMap)
  }

  def sendMsg(edgeTriplet: EdgeTriplet[VertexAttr, Long]): Iterator[(VertexId, Message)] = {
    val srcId = edgeTriplet.srcId
    val dstId = edgeTriplet.dstId
    val srcAttr = edgeTriplet.srcAttr

    val srcName = srcAttr.name
    val pathsUntilSrc: Iterable[Paths] = srcAttr.pathMap.values
    val pathsToSend: Paths = pathsUntilSrc.reduceLeftOption((acc, y) => acc union y).getOrElse(Set(List.empty)).map(x => srcName :: x)

    if (srcAttr.isSource) {
      return Iterator((dstId, Map(srcId -> pathsToSend)))
    }

    if (srcAttr.pathMap.isEmpty) {
      // node is not source and does not have updates hence no need to send messages
      return Iterator.empty
    }

    val pathsUntilDst: Message = edgeTriplet.dstAttr.pathMap
    val sourceMsgInDst: Paths = pathsUntilDst.getOrElse(srcId, Set.empty[List[String]])

    // To know if src and dest have the same paths, we ignore the order to accommodate for loops
    val filerSrcDst = (paths: Paths, name: String) => paths.filter(x => x.count(y => y.equals(name)) < 2).map(x => x.toSet)
    val orderFreeMsgInDest = filerSrcDst(sourceMsgInDst, srcName)
    val pathsUpToDate = filerSrcDst(pathsToSend, srcName).forall(x => orderFreeMsgInDest.contains(x))
    //    Send message if dest does not have the latest state
    if (pathsUpToDate) Iterator.empty else Iterator((dstId, Map(srcId -> pathsToSend)))
  }

  def mergeMsg(x: Message, y: Message): Message = {
    x ++ y
  }

  def allPaths(spark: SparkSession, g: Graph[VertexAttr, VertexId], maxIter: Int): DataFrame = {
    import spark.implicits._
    val graphPaths = g.pregel(initMsg, maxIter, EdgeDirection.Out)(vProg, sendMsg, mergeMsg)
    val computedPaths: Graph[(String, Paths), Long] = graphPaths.mapVertices((_, attr) => (attr.name, attr.pathMap.values.reduceLeftOption((x, y) => x union y).getOrElse(Set.empty[List[String]])))
    val df = computedPaths.vertices.toDF("id", "vattr")
    df.selectExpr("id", "vattr._1 as name", "vattr._2 as paths")
  }

  def main(args: Array[String]): Unit = {
    val sessionBuilder = SparkSession.builder().master("local[*]")
    val spark = sessionBuilder.appName("All Paths").getOrCreate()

    spark.sparkContext.setCheckpointDir("/tmp/lineage_graph/" + java.util.UUID.randomUUID().toString)
    spark.conf.set("spark.graphx.pregel.checkpointInterval", 20)

    val initialGraph: Graph[String, VertexId] = randomGraph(spark, 20)
    val createVertexAttr: (VertexId, String, Option[PartitionID]) => VertexAttr = (_: VertexId, data: String, inDeg: Option[Int]) => VertexAttr(data, inDeg.getOrElse(0) == 0, Map[VertexId, Paths]())
    val graph: Graph[VertexAttr, VertexId] = initialGraph.outerJoinVertices(initialGraph.inDegrees)(createVertexAttr)

    if (graph.vertices.filter(x => x._2.isSource).count() == 0) {
      println("This is not a DAG hence optimization will not work")
    }

    println(graph.vertices.collect().mkString("Array(", ", ", ")"))

    allPaths(spark, graph, 10).show(200, truncate = false)

    //    val pg = PageRank.runWithOptions(graph.reverse, 10)
    //    println(pg.vertices.collect().sortBy(x => x._1).map(x => (x._1, x._2)).mkString("Array(", ", ", ")"))
  }
}
