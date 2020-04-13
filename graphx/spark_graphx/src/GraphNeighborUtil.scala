import org.apache.spark.graphx.{Graph, VertexRDD}
import scala.collection.immutable.HashSet

class GraphNeighborUtil {
	def getFirstNeighborIds(id:Long,graph:Graph[Int,Int]):HashSet[Long]={
		//aggregateMessages[Int]发送给每条边的每个顶点Int类型的消息
		val firstNeighbor:VertexRDD[Int]=graph.aggregateMessages[Int](triplet=>{
			if(triplet.srcId==id){
				triplet.sendToDst(1)
			}
		}, _ + _ )     //聚合相同顶点接收到的消息
		var firstIds=new HashSet[Long]()
		firstNeighbor.collect().foreach(a => firstIds+=a._1)
		firstIds
	}

	def getSecondNeighborIds(firstIds:HashSet[Long] , graph:Graph[Int,Int]):HashSet[Long]={
		var secondIds=new HashSet[Long]()
		firstIds.foreach(id=>{
			val secondNeighbors=getFirstNeighborIds(id,graph)
			secondNeighbors.foreach(secondId=>secondIds+=secondId)
		})
		secondIds
	}

	def getIds(id:Long,graph:Graph[Int,Int]):HashSet[Long]={
	  var tmpIds1=getFirstNeighborIds(id,graph)
		var tmpIds2=getSecondNeighborIds(tmpIds1,graph)
		var Ids=tmpIds1++tmpIds2
		Ids+=id
		Ids
	}

}
