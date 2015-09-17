package com.test

import java.io.File

import scala.util.Random

object TripleProcess extends App {

  // 1st : try to hash the id
  // 2nd : store the id : Int -> name : String relation
  // 3rd : if collide, store non-collide name -> id relation
  val source = scala.io.Source.fromFile("F:\\Download\\0.txt")
  val lines = try source.getLines.toList finally source.close()

  //println(lines.head)
  val result = lines.map { line => line.split(" ") match {
    case Array(subject, predicate, obj) =>
//      val subId = storeSubject(subject)
//      val preId = storePredicate(predicate)
//      val objId = storeSubject(obj)
//      //println(s"$subId $preId $objId")
//      s"$subId $preId $objId"
      ""
    case _ => ""
  }
  }
  println(result.take(5))


}

object DB {
//  val db : DB = DBMaker.newFileDB(new File("tridb"))
//    .closeOnJvmShutdown()
//    .make()
//  val vertices : JMap[Integer,String] = db.getHashMap("idToName-Vertex")
//  val vertexNames : JMap[String,Integer] = db.getHashMap("nameToid-Vertex")
//  val edges : JMap[Integer,String] = db.getHashMap("idToName-Edge")
//  val edgeNames : JMap[String,Integer] = db.getHashMap("nameToid-Edge")
//
//  def storeSubject(str : String) : Int = {
//    val hash = str.hashCode
//    if(vertices.containsKey(hash)) {
//      if(!vertices.get(hash).equals(str)) {
//        // collide
//        if(vertexNames.containsKey(str))
//          return vertexNames.get(str)
//
//        // new
//        var guess = Random.nextInt
//        while(vertices.containsKey(guess)) {
//          guess = Random.nextInt
//        }
//        vertices.put(guess, str)
//        vertexNames.put(str, guess)
//        return guess
//      }
//    } else {
//      vertices.put(hash, str)
//      vertexNames.put(str, hash)
//    }
//    hash
//  }
//
//  def storePredicate(str : String) : Int = {
//    val hash = str.hashCode
//    if(edges.containsKey(hash)) {
//      if(!edges.get(hash).equals(str)) {
//        // collide
//        if(edgeNames.containsKey(str))
//          return edgeNames.get(str)
//
//        // new
//        var guess = Random.nextInt
//        while(edges.containsKey(guess)) {
//          guess = Random.nextInt
//        }
//        edges.put(guess, str)
//        edgeNames.put(str, guess)
//        return guess
//      }
//    } else {
//      edges.put(hash, str)
//      edgeNames.put(str, hash)
//    }
//    hash
//  }
}
