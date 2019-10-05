import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.io.Source


object MainObj extends  App {
  val conf = new SparkConf()
    .setAppName("SparkTester")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val listSpells = get_n_spells(2,200)    // Get n (first param) +1 spell from ID 'first_id'

  var array_level = listSpells.toArray.map(current_spell => {
    val levelwords = current_spell.level.split(" |, ")
    levelSTR_toArray(levelwords)
  })

  @throws(classOf[Exception])
  def levelSTR_toArray(arg:Array[String])={
    var current_string:String = new String("")
    var current_num:Integer = 0
    val list_levels = arg.map(current => {
      try{
        current_num = Integer.parseInt(current)
      }catch {
        case exception: Exception => {
          if(current_string==""){
            current_string = new String(current)
          }else{
            throw new Exception("error in input : two string level in a row")
          }
        }
      }finally {
        if(current_string != "" && current_num != 0){
          var current_spell:(String,Integer) = new Tuple2[String,Integer](current_string,current_num)
          current_string = ""
          current_num=0
          current_spell
        }
      }

    })
    var test = 0
    list_levels
  }

  def get_n_spells(n:Integer,first_id:Integer)={
    var listSpells = new ListBuffer[Spell]
    val url_base = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID="
    for(i <- first_id.toInt to (first_id+n) ){
      var html = Source.fromURL(url_base+(first_id+i))
      var s = html.mkString
      listSpells+=(new Spell(s))
    }
    println(listSpells.toString())
    listSpells
  }
}

class Spell(spellString:String, var content:String = "none", var name: String = "none",
            var level:String = "none", var component: String = "none",
            var description:String = "none") {

  def get_content():Boolean={
    val indexOf1 =spellString.indexOf("<!-- START Spell -->")
    if(indexOf1 == -1) { content=""; return false }
    content=spellString.substring(spellString.indexOf("<!-- START Spell -->"),spellString.indexOf("<!-- END Spell -->"))
    true
  }

  def get_name()={
    name = content.substring(content.indexOf("<P>")+3,content.indexOf("</p>"))
    name
  }

  def get_level()={
    level = content.substring(content.indexOf("Level")+10,content.lastIndexOf("Casting")-55)
    level
  }

  def get_component() ={
    component = content.substring(content.indexOf("Components")+15,content.indexOf("Effect")-22)
    component
  }

  def get_description() ={
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