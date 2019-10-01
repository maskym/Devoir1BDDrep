import scala.io.Source
import util.control.Breaks._

/*
Crawler de spells, créant un objet Spell(nom, level, composantes, description)
Spells allant de 1 à 1975 sur http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=
 */
object MainObj extends App {

  val listeSpells =  new Array[Spell](1985)
  for (i <- 1 to 1975) {
    breakable {
      val html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i)
      val s = html.mkString
      var spell = new Spell(s)
      if(spell.content!="") { // Si la page du sort est vide
        listeSpells(i)= spell
      }
    }
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