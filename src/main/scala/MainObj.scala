import scala.io.Source
import java.io.PrintWriter
/*
Spells allant de 1 à 1975 sur http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=

Remarques d'exécution :
- pb pour récupérer DESCRIPTION sort 92 : se termine par un </table></p></div> et non un .</p></div>
----> DESCRIPTION non testée pour tous les sorts après 92

- pb pour récupérer SPELL LEVEL CLASS NAME sort 1652 : le mot Casting est dans le nom du sort


 */
object MainObj extends App {

   new PrintWriter("spells.txt") {
    for (i <- 1650 to 1652) {
      val html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i)
      val s = html.mkString
      var spell = new Spell(s)
      println(spell.name + "\n" + spell.spell_level.class_name + "\n" + spell.component + "\n\n")
    }
    close()
  }

}


class Spell(spellString:String, var content:String = "none", var name: String = "none",
            var spell_level:SpellLevel = new SpellLevel, var component: String = "none",
            var description:String = "none") {

  def get_content()={
    content=spellString.substring(spellString.indexOf("<!-- START Spell -->"),spellString.indexOf("<!-- END Spell -->"))
    content
  }

  def get_name()={
    name = content.substring(content.indexOf("<P>")+3,content.indexOf("</p>"))
    name
  }

  def get_level():SpellLevel={
    var level:SpellLevel = new SpellLevel
    level.class_name = content.substring(content.indexOf("Level")+10,content.indexOf("Casting")-22)
    level
  }

  def get_component() ={
    component = content.substring(content.indexOf("Components")+15,content.indexOf("Effect")-22)
    component
  }

  def get_description() ={
    description = content.substring(content.indexOf("SPDesc")+11,content.indexOf(".</p></div>"))
    description
  }



  content = get_content()
  name = get_name()
  //spell_level = get_level()
  component = get_component()
  //description = get_description()

}
class SpellLevel {
  var class_name:String="none"
  var class_level:Integer=1
}
