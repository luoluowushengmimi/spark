package com.test.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object allPathsUndirected {
  def main(args: Array[String]) {

    val originalId = "http://csdb.material/gene/jca1"
    val destId = "http://csdb.material/pathway/2"
    val datafile = "D:/project/graphx/src/links2.nt"



    import org.apache.spark.graphx._
    // Import random graph generation library
    import org.apache.spark.graphx.util.GraphGenerators

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    // read file
    val textFile  = sc.textFile(datafile)


    val srcids = textFile.map(_.split(" ")(0))
    val vetnames = textFile.map(_.split(" ")(2)).union(srcids).distinct().zipWithIndex()
    val vetids: RDD[(VertexId, String)] = vetnames.map{ case (a,b) => (b,a)}
    val vetmap = vetnames.collectAsMap()
    val vetidmap = vetids.collectAsMap()

    val sourceId: VertexId = vetmap.get(originalId).get // The ultimate source
    val destinationId: VertexId = vetmap.get(destId).get // The ultimate source


    //    val users: RDD[(VertexId, (String, String))] =
//      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
//        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
val relationships: RDD[Edge[String]] =
textFile.map(_.split(" ")).map { arr =>
    Edge(vetmap.get(arr(0)).get, vetmap.get(arr(2)).get, arr(1))
}


//  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
//    Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))


    val graph: Graph[String, String] = Graph(vetids, relationships, "")
    // A graph with edge attributes containing distances
//    val graph: Graph[Long, Int] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 5).groupEdges(Math.max)
    var uni = graph.edges.reverse.union(graph.edges).distinct().filter{edge => edge.srcId != edge.dstId}

    val edges : Map[Long, List[Edge[String]]]= uni.groupBy(_.srcId).collect().toMap.mapValues(_.toList)
    edges.foreach{case (id, egs) =>
      println(id + " : " + egs.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
    }

//    reverse.foreach{case (id, egs) =>
//      println("reverse - " + id + " : " + egs.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//    }

    var whole : List[List[Edge[String]]] = Nil


    // Initialize the graph such that all vertices except the root have distance infinity.
    var last : List[List[Edge[String]]] = graph.edges.filter(_.srcId == sourceId).map(_ :: Nil).collect().toList
    whole = last ::: whole
    whole.foreach{lst=>
      println("whole1 " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
    }


    var l = 2
    while (l < 7){

      var last2 : List[List[Edge[String]]]= Nil
      last.map{alst =>
        val newSrcId = alst.head.dstId
        val newEdges : Option[List[Edge[String]]] = edges.get(newSrcId)
        newEdges.foreach{ newegs : List[Edge[String]] =>
          newegs.foreach{neweg : Edge[String] =>
            if(!alst.contains(neweg) && !alst.contains(neweg.copy(srcId = neweg.dstId, dstId = neweg.srcId))) {
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
//    whole.foreach{lst=>
//      println("whole" + l + " " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//    }

    whole.filter(_.head.dstId == destinationId).foreach{lst=>
//      println("result in number " + l + " " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
      println("result " + lst.reverse.map(eg => vetidmap.get(eg.srcId).get + " -> " + eg.attr + " -> "+  vetidmap.get(eg.dstId).get).mkString(", "))
    }
  }
}
