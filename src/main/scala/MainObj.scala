import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.io.Source


object MainObj extends  App {
  val conf = new SparkConf()
    .setAppName("SparkTester")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val listSpells = get_n_spells(1975,1)
  var sqltest = createSQLdb(listSpells)

  // Fonction pour table SQL des spells
  def createSQLdb(listSpells:ListBuffer[(Integer,String, String, String)]): Unit ={

    // Initialisation de session SQL
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparksql = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    // Conversion de liste des spells en RDD et conversion du RDD en DataFrame pour SQL
    val listSpellsRDD = sc.makeRDD(listSpells)
    val spellsDF = listSpellsRDD.toDF("id","name","level","component")

    // Création et affichage de la vue SQL SPELL
    spellsDF.createOrReplaceTempView("SPELL")
    val spellsaffichDF = sparksql.sql("""SELECT * FROM SPELL WHERE component='V' AND (level LIKE '%wizard 0%' OR level LIKE '%wizard 1%' OR level LIKE '%wizard 2%' OR level LIKE '%wizard 3%' OR level LIKE '%wizard 4%')""")
    spellsaffichDF.show(200,false)
  }

  def get_n_spells(n:Integer,first_id:Integer)={
    var listSpells = new ListBuffer[(Integer,String,String,String)]
    val url_base = "file:///C:/output/spell_"
    var url_end = ".html"
    for(i <- 0 until n ){
      var html = Source.fromURL(url_base+(first_id+i)+url_end)
      var s = html.mkString
      var spell = new Spell(s, first_id+i)
      var spell_ID= first_id+i
      listSpells += new Tuple4[Integer, String, String, String](spell_ID, spell.name, spell.level, spell.component )
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