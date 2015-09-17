package com.test.graphx

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gyqgd on 15-5-31.
 */
object allPaths {
  def main(args: Array[String]) {

    import org.apache.spark.graphx._
    // Import random graph generation library
    import org.apache.spark.graphx.util.GraphGenerators

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf)

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).groupEdges(Math.max)


    val edges : Map[Long, List[Edge[Int]]]= graph.edges.groupBy(_.srcId).collect().toMap.mapValues(_.toList)
    edges.foreach{case (id, egs) =>
      println(id + " : " + egs.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
    }

    var whole : List[List[Edge[Int]]] = Nil

    val sourceId: VertexId = 1 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    var last : List[List[Edge[Int]]] = graph.edges.filter(_.srcId == sourceId).map(_ :: Nil).collect().toList
    whole = last ::: whole
    whole.foreach{lst=>
      println("whole1 " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
    }


    var l = 2
    while (l < 7){

      var last2 : List[List[Edge[Int]]]= Nil
      last.map{alst =>
        val newSrcId = alst.head.dstId
        val newEdges : Option[List[Edge[Int]]] = edges.get(newSrcId)
        newEdges.foreach{ newegs : List[Edge[Int]] =>
          newegs.foreach{neweg : Edge[Int] =>
            if(!alst.contains(neweg)) {
              last2 = (neweg :: alst) :: last2
            }
          }
        }
      }

      last = last2
      whole = last2 ::: whole
//      whole.foreach{lst=>
//        println("whole" + l + " " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//      }

      l = l + 1
    }

//    edges.foreach{case (id, egs) =>
//      println(id + " : " + egs.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//    }
    whole.foreach{lst=>
      println("whole" + l + " " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
    }



  }
}
