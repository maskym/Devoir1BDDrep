import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object PageRankCalculer extends  App {

  val conf = new SparkConf()
    .setAppName("SparkTester")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val d = 0.85

  /** A node of a graph.
   * Freely inspired from "case class sommet(pagerank : Double, adjlist : Array[Int])" from the subject
   * @param id      Node's ID
   * @param PR      Default PR score
   * @param adjlist List of adjacent nodes
   */
  class node(var id: String, var PR: Double = 1.0, var adjlist:Array[String]) extends Serializable {
    override def toString: String = s"id : $id PR : $PR adjlist : $adjlist"
  }

  var myGraph = sc.makeRDD(Array(
    (1L, new node(id = "A", PR = 1.0, Array[String]("B","C") ) ), //A
    (2L, new node(id = "B", PR = 1.0, Array[String]("C"))), //B
    (3L, new node(id = "C", PR = 1.0, Array[String]("A"))), //C
    (4L, new node(id = "D", PR = 1.0, Array[String]("C"))) //D
  ))

  for (i <- 0 to 20) {
    var resultGraph = myGraph.map(currentNode => {
      var previousNodes = getPreviousNodes(currentNode)
      computePR(currentNode,previousNodes)
      currentNode

    })

    var resultGraphCollected = resultGraph.collect()
    myGraph = sc.makeRDD(resultGraphCollected)

    println("**************************\n**************************\nITERATEUR : "+i)
    for(j <- myGraph.collect().indices){
      println("{id : "+myGraph.collect()(j)._2.id+" pageRank : "+myGraph.collect()(j)._2.PR+"}")
    }
    var test = 50
  }

  var collected = myGraph.collect()
  var test = 50

  /**
   * Get nodes before 'currentNode'
   * @param currentNode Node to get previous nodes from.
   * @return ListBuffer[(Long,node)] containing previous nodes of 'currentNode'
   */
  def getPreviousNodes(currentNode:(Long, node)) = {
    var previousNodes = new ListBuffer[(Long, node)];
    var myGraphCollected = myGraph.collect()
    for (i <- myGraphCollected.indices) {
      if (myGraphCollected(i)._2.adjlist contains (currentNode._2.id)) {
        previousNodes += myGraphCollected(i)
      }
    }
    previousNodes
  }

  /**
   * Calculate PageRank of 'currentNode'
   * @param currentNode Calculating PageRank on this node.
   * @param previousNodes Nodes before 'currentNode'
   * @return (Long,Node) : 'currentNode' with its PageRank updated
   */
  def computePR(currentNode:(Long, node), previousNodes:ListBuffer[(Long,node)])={
    var sum = 0.0
    for (i <- previousNodes.indices){
      sum += (previousNodes(i)._2.PR / previousNodes(i)._2.adjlist.length)
    }
    var PR = (1-d)
    var step = d*sum
    PR +=step
    currentNode._2.PR = PR
    currentNode
  }
}