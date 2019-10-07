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

  val listSpells = get_n_spells(2,7)

  val tuple2_list = get_tuple2array_from_spells()       // convert full strings for levels to Tuple2(class:String,num:Integer)
  var reduced_array = filter_spells(1,"(W|w)izard") // remove spells under level "max_lvl" and not from class "class_str"
  var collected = reduced_array.collect()

  def filter_spells(max_lvl:Integer,class_str:String)={
    sc.makeRDD(tuple2_list).filter(current_spell => {
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

  def get_tuple2array_from_spells()={
    listSpells.toArray.map(current_spell => {
      levelSTR_toArray(current_spell.level.split(" |, "))
    })
  }

  @throws(classOf[Exception])
  def levelSTR_toArray(arg:Array[String])={
    var list_levels = new ListBuffer[(String, Integer)]
    var current_string:String = new String("")
    var current_num:Integer = 0
    for(i <- arg.indices){
      try{
        current_num = Integer.parseInt(arg(i))
      }catch {
        case exception: Exception =>
          if(current_string==""){
            current_string = new String(arg(i))
          }else{
            throw new Exception("error in input : two string level in a row")
          }

      }finally {
        if(current_string != "" && current_num != 0){
          var current_spell:(String,Integer) = new Tuple2[String,Integer](current_string,current_num)
          list_levels+=current_spell
          current_string = ""
          current_num=0
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
      listSpells+=new Spell(s,i)
    }
    listSpells
  }
}

class Spell(spellString:String, spellID:Integer, var content:String = "none", var name: String = "none",
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
    level = content.substring(content.indexOf("Level")+10,content.lastIndexOf("Casting")-55)
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
  }
}