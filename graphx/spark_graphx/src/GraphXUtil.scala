import org.apache.spark.graphx.{EdgeRDD, Graph, VertexRDD}

import scala.collection.immutable.HashSet

class GraphXUtil {

	def getSubGraphXVertices(graph:Graph[Int,Int],ids:HashSet[Long]): VertexRDD[Long] ={
		val vertexRDD=graph.subgraph(vpred=(id,attr)=>ids.contains(id))
		vertexRDD.vertices.mapValues((_,attr)=>attr.toLong)
	}

	def getSubGraphXEdges(graph:Graph[Int,Int],ids: HashSet[Long]):EdgeRDD[Long]={
		val edgeRDD=graph.subgraph(vpred=(id,attr)=>ids.contains(id))
		edgeRDD.edges.mapValues(edge=>edge.attr.toLong)
	}

}
