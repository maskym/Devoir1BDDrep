import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex


object MainObj extends  App {
  val conf = new SparkConf()
    .setAppName("SparkTester")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val listSpells = get_n_spells(11,1)

  val tuple3_list = get_tuple3array_from_spells()       // convert full strings for levels to Tuple2(class:String,num:Integer)
  //var reduced_array = filter_spells(1,"(W|w)izard") // remove spells under level "max_lvl" and not from class "class_str"
  //var collected = reduced_array.collect()

  var testsql = createSQLdb(tuple3_list)

  var test = 50

  // Pour l'instant le résultat du select affiche les 11 arrays stockés dans le RDD, pas possible de déballer les array en SQL
  // Il faut créer une autre structure de données à partir de tuple3 pour pouvoir recréer un map qui sera bien interprété en SQL pour des commandes
  def createSQLdb(tuple3: Array[ListBuffer[(String, Integer, Integer)]]): Unit ={

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import sqlContext.implicits._


    /*for(j <- 1 until 10) {
      for(i <- tuple3(j).indices){
        var current_classlvl = tuple3(j)(i)._1
        var current_lvl = tuple3(j)(i)._2
        var current_id = tuple3(j)(i)._3
        var current_spell = 0
      }
    }*/

    var tuple3RDD = sc.makeRDD(tuple3)
    val spellsDF = tuple3RDD.toDF()

    // Création de la vue SQL SPELL
    spellsDF.createOrReplaceTempView("SPELL")
    val spellsaffichDF = spark.sql("SELECT * FROM SPELL")
    spellsaffichDF.show()
    // Pour l'instant le résultat du select affiche les 11 arrays stockés dans le RDD, pas possible de déballer les array en SQL

    /*for(j <- 1 until 10) {
      for(i <- tuple3(j).indices){
        var current_classlvl = tuple3(j)(i)._1
        var current_lvl = tuple3(j)(i)._2
        var current_id = tuple3(j)(i)._3
        var current_spell = 0
      }
    }*/


    test=0
  }



  def filter_spells(max_lvl:Integer,class_str:String)={
    sc.makeRDD(tuple3_list).filter(current_spell => {
      val pattern = new Regex(class_str)
      var lvl_ok = false
      var class_ok = false
      for(i <- current_spell.indices){
        if(current_spell(i)._2 <= max_lvl){lvl_ok = true}
        if( pattern.findAllMatchIn(current_spell(i)._1).toString == "non-empty iterator" ){class_ok=true}
      }
      var result = lvl_ok&&class_ok
      result
    })
  }

  def get_tuple3array_from_spells()={
    listSpells.toArray.map(current_spell => {
      levelSTR_toArray(current_spell.level.split(" |, "),current_spell.spell_ID)
    })
  }

  @throws(classOf[Exception])
  def levelSTR_toArray(arg:Array[String],spell_ID:Integer)={
    var list_levels = new ListBuffer[(String, Integer,Integer)]
    var current_string:String = new String("")
    var current_num:Integer = -1
    for(i <- arg.indices){
      try{
        current_num = Integer.parseInt(arg(i))
      }catch {
        case exception: Exception =>
          if(current_string==""){
            current_string = new String(arg(i))
          }else{
            throw new Exception("error in input n°"+i+" : two string level in a row")
          }

      }finally {
        if(current_string != "" && current_num != -1){
          var current_spell:(String,Integer,Integer) = new Tuple3[String,Integer,Integer](current_string,current_num,spell_ID)
          list_levels+=current_spell
          current_string = ""
          current_num = -1
        }
      }
    }
    list_levels
  }

  def get_n_spells(n:Integer,first_id:Integer)={
    var listSpells = new ListBuffer[Spell]
    val url_base = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID="
    for(i <- 0 until n ){
      var html = Source.fromURL(url_base+(first_id+i))
      var s = html.mkString
      listSpells+=new Spell(s,(first_id+i))
      var spellencours= first_id+i
      println("i : " + spellencours) // Temps réel de la récupération des sorts
    }
    listSpells
  }
}

class Spell(spellString:String, spellID_arg:Integer,var spell_ID:Integer = 0, var content:String = "none", var name: String = "none",
            var level:String = "none", var component: String = "none",
            var description:String = "none") {

  def get_content():Boolean={
    val indexOf1 =spellString.indexOf("<!-- START Spell -->")
    if(indexOf1 == -1) { content=""; return false }
    content=spellString.substring(spellString.indexOf("<!-- START Spell -->"),spellString.indexOf("<!-- END Spell -->"))
    true
  }

  def get_name(): String ={
    name = content.substring(content.indexOf("<P>")+3,content.indexOf("</p>"))
    name
  }

  def get_level(): String ={
    var substringed = content.substring(content.indexOf("Level")+10);
    level = substringed.substring(0,substringed.indexOf("</p>"))
    level
  }

  def get_component(): String ={
    component = content.substring(content.indexOf("Components")+15,content.indexOf("Effect")-22)
    component
  }

  def get_description(): String ={
    description = content.substring(content.indexOf("SPDesc")+11,content.lastIndexOf("</p></div>"))
    description
  }

  get_content()
  if(content!=""){ // Si la page du sort est vide
    name = get_name()
    level = get_level()
    component = get_component()
    description = get_description()
    spell_ID = spellID_arg

  }
}