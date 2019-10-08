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

  var time_debut = System.currentTimeMillis()

  val listSpells = get_n_spells(1,275)

  if(listSpells(0).component == "V"){
    println("correc'")
  }
  val tuple4_list = get_tuple4array_from_spells()       // convert full strings for levels to Tuple2(class:String,num:Integer)
  var filtered_array = filter_spellscomponent()
  var reduced_array = filter_spells(1,"(W|w)izard","V") // remove spells under level "max_lvl" and not from class "class_str"
  var collected = reduced_array.collect()

  var test = 50

  def filter_spellscomponent()

  def filter_spells(max_lvl:Integer,class_str:String,component:String)={
    sc.makeRDD(tuple4_list).filter(current_spell => {
      val pattern = new Regex(class_str)
      var lvl_ok = false
      var class_ok = false
      var component_ok = false
      for(i <- current_spell.indices){
        if(current_spell(i)._2 <= max_lvl){lvl_ok = true}
        if( pattern.findAllMatchIn(current_spell(i)._1).toString == "non-empty iterator" ){class_ok=true}
        if( current_spell(i)._4 == "V"){
          component_ok = true
        }
      }
      var result = lvl_ok&&class_ok&&component_ok
      result
    })
  }

  def get_tuple4array_from_spells()={
    listSpells.toArray.map(current_spell => {
      levelSTR_toArray(current_spell.level.split(" |, "),current_spell.spell_ID,current_spell.component)
    })
  }

  @throws(classOf[Exception])
  def levelSTR_toArray(arg:Array[String],spell_ID:Integer,component:String)={
    var list_levels = new ListBuffer[(String, Integer,Integer,String)]
    var current_string:String = new String("")
    var current_num:Integer = -1
    var curseur = 0
    println("arg.size : "+arg.length)
    while(curseur < arg.length){
      if(new Regex("[0-9]/[aA-zZ]").findAllMatchIn(arg(curseur)).toString() == "non-empty iterator"){
        if(current_num== -1&&current_string!=""){
          current_num = Integer.parseInt(arg(curseur).split("/")(0))
          curseur-=1
        }else if(current_num== -1&&current_string==""){
          current_string = arg(curseur).split("/")(1)
        }
      }else {
        try {
          current_num = Integer.parseInt(arg(curseur))
        } catch {
          case exception: Exception =>
            if (current_string == "") {
              current_string = new String(arg(curseur))
            } else {
              throw new Exception("error in input n°" + curseur + "/spell_id : " + spell_ID + " : two string level in a row")
            }
        }
      }

      if(current_string != "" && current_num != -1){
        var current_spell:(String,Integer,Integer,String) = new Tuple4[String,Integer,Integer,String](current_string,current_num,spell_ID,component)
        list_levels += current_spell
        current_string = ""
        current_num = -1
      }
      curseur+=1
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
      println("i : "+i)
      if( (i%100)==0 ){
        println("time : "+(System.currentTimeMillis() - time_debut))
      }
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