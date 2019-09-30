//Petit programme exemple pour 8inf803
//Edmond La Chance

/*
Ce petit programme en scala execute un algorithme de graphe itératif sur Spark GraphX. Cet algorithme
essaie de colorier un graphe avec un nombre de couleurs minimal (mais l'algorithme est très random et ne
donne pas de très bons résultats!). Par contre le code est très court donc il est intéressant comme exemple.
Voici comment ce programme fonctionne :
1. L'exécution commence à testPetersenGraph.
2. On crée le graphe directement dans le code. Le tiebreaking value est une valeur random (ici hardcodée)
qui permet a l'algorithme glouton de coloriage de graphe de trancher dans ses décisions
3. La boucle itérative se trouve dans la fonction execute
4. L'algorithme FC2 fonctionne de la façon suivante :
  Chaque itération, les noeuds du graphe s'envoient des messages. Si on est un noeud, et qu'on trouve un voisin qui a un meilleur
  tiebreak, on doit augmenter notre couleur. Les noeuds qui n'augmentent pas gardent une couleur fixe et
  arrêtent d'envoyer des messages.
  L'algorithme s'arrête lorsqu'il n'y a plus de messages envoyés
 C'est donc un algorithme très simple de coloriage (pas le meilleur).
 */

package FC2 {

  import org.apache.spark.{SparkConf}
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}

  //tiebreak value is generated randomly before every graph iteration
  class node(val id: Int, val color: Int = 1, val knighthood: Boolean = false, val tiebreakValue: Long = 1L) extends Serializable {
    override def toString: String = s"id : $id tiebreakValue : $tiebreakValue color : $color knighthood : $knighthood"
  }

  class FC2 extends Serializable {
    def getChromaticNumber(g: Graph[node, String]): Int = {
      val aa = g.vertices.collect()
      var maxColor = 0
      for (i <- aa) {
        if (i._2.color > maxColor) maxColor = i._2.color
      }
      maxColor
    }

    def sendTieBreakValues(ctx: EdgeContext[node, String, Long]): Unit = {
      if (ctx.srcAttr.knighthood == false && ctx.dstAttr.knighthood == false) {
        ctx.sendToDst(ctx.srcAttr.tiebreakValue)
        ctx.sendToSrc(ctx.dstAttr.tiebreakValue)
      }
    }

    def selectBest(id1: Long, id2: Long): Long = {
      if (id1 < id2) id1
      else id2
    }

    def increaseColor(vid: VertexId, sommet: node, bestTieBreak: Long): node = {
      if (sommet.tiebreakValue < bestTieBreak)
        return new node(sommet.id, sommet.color, true, sommet.tiebreakValue)
      else {
        return new node(sommet.id, sommet.color + 1, false, sommet.tiebreakValue)
      }
    }

    def execute(g: Graph[node, String], maxIterations: Int, sc: SparkContext): Graph[node, String] = {
      var myGraph = g
      var counter = 0
      val fields = new TripletFields(true, true, false) //join strategy

      def loop1: Unit = {
        while (true) {

          println("ITERATION NUMERO : " + (counter + 1))
          counter += 1
          if (counter == maxIterations) return

          val messages = myGraph.aggregateMessages[Long](
            sendTieBreakValues,
            selectBest,
            fields //use an optimized join strategy (we don't need the edge attribute)
          )

          if (messages.isEmpty()) return

          myGraph = myGraph.joinVertices(messages)(
            (vid, sommet, bestId) => increaseColor(vid, sommet, bestId))

          //Ignorez : Code de debug
          var printedGraph = myGraph.vertices.collect()
          printedGraph = printedGraph.sortBy(_._1)
          printedGraph.foreach(
            elem => println(elem._2)
          )
        }

      }

      loop1 //execute loop
      myGraph //return the result graph
    }
  }

  object testPetersenGraph extends App {
    val conf = new SparkConf()
      .setAppName("Petersen Graph (10 nodes)")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var myVertices = sc.makeRDD(Array(
      (1L, new node(id = 1, tiebreakValue = 5)), //A
      (2L, new node(id = 2, tiebreakValue = 4)), //B
      (3L, new node(id = 3, tiebreakValue = 9)), //C
      (4L, new node(id = 4, tiebreakValue = 7)), //D
      (5L, new node(id = 5, tiebreakValue = 2)), //E
      (6L, new node(id = 6, tiebreakValue = 3)), //F
      (7L, new node(id = 7, tiebreakValue = 10)), //G
      (8L, new node(id = 8, tiebreakValue = 6)), //H
      (9L, new node(id = 9, tiebreakValue = 1)), //I
      (10L, new node(id = 10, tiebreakValue = 8)))) //J

    var myEdges = sc.makeRDD(Array(
      Edge(1L, 2L, "1"), Edge(1L, 3L, "2"), Edge(1L, 6L, "3"),
      Edge(2L, 7L, "4"), Edge(2L, 8L, "5"),
      Edge(3L, 4L, "6"), Edge(3L, 9L, "7"),
      Edge(4L, 5L, "8"), Edge(4L, 8L, "9"),
      Edge(5L, 6L, "10"), Edge(5L, 7L, "11"),
      Edge(6L, 10L, "12"),
      Edge(7L, 9L, "13"),
      Edge(8L, 10L, "14"),
      Edge(9L, 10L, "15")
    ))

    var myGraph = Graph(myVertices, myEdges)
    val algoColoring = new FC2()
    val res = algoColoring.execute(myGraph, 20, sc)
    println("\nNombre de couleur trouvées: " + algoColoring.getChromaticNumber(res))
  }
}