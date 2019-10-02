import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.io.Source

object spark_tester extends App {
  val conf = new SparkConf()
    .setAppName("SparkTester")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  //val inputfile = sc.textFile("input.txt")
  val classes_list = Array("cleric/oracle","witch","inquisitor","druid","sorcerer/wizard")
  val listSpells = new Array[Spell](2)
  var html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=201")
  var s = html.mkString
  listSpells(0) = new Spell(s)
  html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=202")
  s = html.mkString
  listSpells(1) = new Spell(s)


  //var z = Array("Zara", "Nuha", "Ayan","Zara","Zara")
  val v1 = listSpells.flatMap(current_spell => {
    var templFlatMap = current_spell.level.split(" |,")
    var spell_class:String="";
    var level:Integer = 0;
    var resultats= new List();
    for(i <- 0 to templFlatMap.size){
      if(templFlatMap(i)!="") {
        if(level == 0 || spell_class == ""){
          try {
            level = Integer.parseInt(templFlatMap(i))
          } catch {
            case exception: Exception =>
              if(templFlatMap(i).length >= 1){
                spell_class = templFlatMap(i)
              }
          }
        }else{
          resultat = new Tuple2(spell_class,level)
        }

      }
    }
    var str:String=""
    st1
  }  )
  //println("v1 : "+v1(0))

  /*val v1 = inputfile.flatMap(line => line.split(" "))
  val v1collect = v1.collect();
  val v2 = v1.map(word => (word, 1))
  val v2collect = v2.collect()
  val counts = v2.reduceByKey(_+_)
  val countscollect = counts.collect()*/
  val test=0

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
