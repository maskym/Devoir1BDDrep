import scala.io.Source
object MainObj extends  App {
    val html = Source.fromURL("http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=1")
    val s = html.mkString
    println(s)
    println("s.indexOf(\"START\") : "+s.indexOf("START"))
}