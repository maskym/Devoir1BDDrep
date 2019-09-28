import org.spark_project.guava.primitives.UnsignedInteger

import scala.io.Source
object MainObj extends  App {
    val html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=1")
    val s = html.mkString
    var spell = new Spell(s)
    println(spell.content)
    println(spell.name)
}

class Spell(spellString:String) {
    var name = "none"
    var spell_level:SpellLevel = new SpellLevel
    var component = "none"
    var spellResistance:Boolean = false
    var content:String = "none"
    def get_name()={
        name=content.substring(content.indexOf("<P>")+3,content.indexOf("</p>"))
        name
    }
    def get_level():SpellLevel={
        var level:SpellLevel = new SpellLevel
        level.class_name = content.substring(content.indexOf("Level")+10,content.indexOf("Sep1")-16)
        level
    }
    content = spellString.substring(spellString.indexOf("<!-- START Spell -->"),spellString.indexOf("<!-- END Spell -->"))
    name = get_name()
    spell_level = get_level()
}
class SpellLevel {
    var class_name:String="none"
    var class_level:Integer=1
}