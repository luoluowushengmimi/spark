package io.traverse

/**
 * Created by gyqgd on 15-8-31.
 */
class Xrain extends App {
  import scala.io.Source
  val s = Source.fromFile("/Users/gyqgd/Desktop/xdata/id/iri/go/0.txt","UTF-8").getLines.take(2).foreach {
    println(_)
  }
}
