package com.test.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AllPathes {

   def getAllPathes(origin:String, dest:String, sc:SparkContext) = {
     import org.apache.spark.graphx._
     // Import random graph generation library

//     val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
//     val sc = new SparkContext(conf)

     // read file
     import com.typesafe.config.ConfigFactory

     val myconf = ConfigFactory.load()
     val textFile  = sc.textFile(myconf.getString("datafile")).map(_.replaceAll("\\s+", " "))
     val pathlength = myconf.getInt("path.length")


     val srcids = textFile.map(_.split(" ")(0))
     val vetnames = textFile.map(_.split(" ")(2)).union(srcids).distinct().zipWithIndex()
     val vetids: RDD[(VertexId, String)] = vetnames.map{ case (a,b) => (b,a)}
     val vetmap = vetnames.collectAsMap()
     val vetidmap = vetids.collectAsMap()
     val relationships: RDD[Edge[String]] =
       textFile.map(_.split(" ")).map { arr =>
         Edge(vetmap.get(arr(0)).get, vetmap.get(arr(2)).get, arr(1))
       }
     val graph: Graph[String, String] = Graph(vetids, relationships, "")
     // A graph with edge attributes containing distances
     //    val graph: Graph[Long, Int] =
     //      GraphGenerators.logNormalGraph(sc, numVertices = 5).groupEdges(Math.max)
     var uni = graph.edges.reverse.union(graph.edges).distinct().filter{edge => edge.srcId != edge.dstId}

     val edges : Map[Long, List[Edge[String]]]= uni.groupBy(_.srcId).collect().toMap.mapValues(_.toList)
//     edges.foreach{case (id, egs) =>
//       println(id + " : " + egs.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//     }


     val sourceId: VertexId = vetmap.get(origin).get // The ultimate source
     val destinationId: VertexId = vetmap.get(dest).get // The ultimate source

     var whole : List[List[Edge[String]]] = Nil


     // Initialize the graph such that all vertices except the root have distance infinity.
     var last : List[List[Edge[String]]] = graph.edges.filter(_.srcId == sourceId).map(_ :: Nil).collect().toList
     whole = last ::: whole
//     whole.foreach{lst=>
//       println("whole1 " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
//     }


     var l = 2
     while (l < pathlength){

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

       l = l + 1
     }


      whole.filter(_.head.dstId == destinationId).map{lst=>
       //      println("result in number " + l + " " + lst.map(eg => eg.srcId + "->" + eg.dstId).mkString(", "))
       lst.reverse.map(eg => vetidmap.get(eg.srcId).get + " " + eg.attr + " "+  vetidmap.get(eg.dstId).get).mkString(", ")
     }.toArray
   }
 }
