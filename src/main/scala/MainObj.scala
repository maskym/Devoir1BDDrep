import scala.io.Source
import java.io.PrintWriter
import util.control.Breaks._

/*
Spells allant de 1 à 1975 sur http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=

Remarques d'exécution :

- pb pour récupérer SPELL LEVEL CLASS NAME sort 1652 : le mot Casting est dans le nom du sort

- spell 1593 ????

- spell 1841 et 1972 n'existe pas !!!!

 */
object MainObj extends App {

   new PrintWriter("spells.txt") {
    for (i <- 1 to 1975) {
      breakable {
        val html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i)
        val s = html.mkString
        var spell = new Spell(s)
        if(spell.content!="") {
          println(spell.name + "\n" + spell.spell_level.class_name + "\n" + spell.component + "\n" + spell.description + "\n\n")
        }
      }
    }
    close()
  }

}


class Spell(spellString:String, var content:String = "none", var name: String = "none",
            var spell_level:SpellLevel = new SpellLevel, var component: String = "none",
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

  def get_level():SpellLevel={
    var level:SpellLevel = new SpellLevel
    level.class_name = content.substring(content.indexOf("Level")+10,content.lastIndexOf("Casting")-55)
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
  if(content!=""){
    name = get_name()
    spell_level = get_level()
    component = get_component()
    description = get_description()
  }

}
class SpellLevel {
  var class_name:String="none"
  var class_level:Integer=1
}
