import org.apache.spark.rdd.RDD
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

  val listSpells = get_n_spells(1970,1)

  // spell structure : Tuple4(ID, name, level, component)

  var target_component = "V"
  var target_class_regex ="(W|w)izard"
  var target_level = 4

  var filtered_tuples = listSpells.filter(current_spell => {
    var result = false
    if(current_spell._4 == target_component){
      if(new Regex(target_class_regex+" [0-"+target_level+"]").findAllMatchIn(current_spell._3).toString() == "non-empty iterator"){
        result = true
      }
    }
    result
  })

  var test = 50

  def get_n_spells(n:Integer,first_id:Integer)={
    var listSpells = new ListBuffer[(Integer,String,String,String)]
    val url_base = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID="
    for(i <- 0 until n ){
      var html = Source.fromURL(url_base+(first_id+i))
      var s = html.mkString
      var spell = new Spell(s,(first_id+i))
      var spell_ID= first_id+i
      listSpells += new Tuple4[Integer, String, String, String](spell_ID, spell.name, spell.level, spell.component )
      println("i : " + spell_ID) // Temps réel de la récupération des sorts
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