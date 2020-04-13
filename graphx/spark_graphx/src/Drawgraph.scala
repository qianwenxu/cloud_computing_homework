import java.io.{File, PrintWriter}
import scala.io.Source
import org.apache.spark.graphx.{Graph, GraphLoader, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object Drawgraph {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

		val conf=new SparkConf()
				.setMaster("local[2]")
				.setAppName("Drawgraph")
		val sc=new SparkContext(conf)

		//构造图
		val graph=GraphLoader.edgeListFile(sc,"data/output.txt")
		graph.cache()
		val graphNeighborUtil=new GraphNeighborUtil

		//获取二级邻居的ids
		val id = 56
		val secondIds=graphNeighborUtil.getIds(id,graph)

		val graphXUtil=new GraphXUtil
		val vertices=graphXUtil.getSubGraphXVertices(graph,secondIds)
		val edges=graphXUtil.getSubGraphXEdges(graph,secondIds)

		val subGraph=Graph(vertices,edges)
		val inputFile = Source.fromFile("data/titlemap.txt")
		val titlelines = inputFile.getLines
		var TitleMap:Map[Long,String] = Map()
		for (titleline <- titlelines){
		  val arr = titleline.split("\\=")
		  TitleMap+=(arr(0).toLong -> arr(1).toString())
		}
		//画图
		val singlegra: SingleGraph = new SingleGraph("graphDemo")
		singlegra.addAttribute("ui.stylesheet","url(style/stylesheet)")
		singlegra.setAttribute("ui.quality")
    singlegra.setAttribute("ui.antialias")
		//    load the graphx vertices into GraphStream
    for ((index, _) <- subGraph.vertices.collect()){
      val node = singlegra.addNode(index.toString).asInstanceOf[SingleNode]
      //node.addAttribute("ui.label", TitleMap.get(index))
      node.addAttribute("ui.label", index.toString)
    }

//    load the graphx edges into GraphStream edges
    for (Edge(x, y, _) <- subGraph.edges.collect()){
      val edge = singlegra.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }

    singlegra.display()

		val firstNeighbor:VertexRDD[Double]=subGraph.pageRank(0.01).vertices

		val neighborRank = firstNeighbor.filter(pred=>{
			var flag=false
			secondIds.foreach(id=>if(id == pred._1) flag = true)
			flag
		}).sortBy(x=>x._2,false)          //按照rank从大到小排序
        		.map(t=>t._1+" "+t._2)
				.coalesce(1)
		neighborRank.cache()
		neighborRank.saveAsTextFile("data/rank_"+id)

		println("second neighbor rank:")
		neighborRank.foreach(println)
		println("top five rank:")
		neighborRank.take(5).foreach(println)

		sc.stop()

	}

}
