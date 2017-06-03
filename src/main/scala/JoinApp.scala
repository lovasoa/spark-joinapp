import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, BasicConfigurator, Level}
import org.apache.spark.network.util.JavaUtils

object Main {
  var spark : SparkSession = null
  def sc = spark.sparkContext
  val logger = Logger.getLogger(Main.getClass)
  var is_debug = false

  def main(args: Array[String]) {
    spark = SparkSession.builder()
      .master("yarn")
      .appName("JoinApp " ++ args.mkString(" "))
      .config("spark.eventLog.enabled", "true")
      .config("spark.driver.maxResultSize", "0")
      .getOrCreate()

    spark.conf.set("spark.driver.maxResultSize",
      spark.conf.get("spark.driver.memory", "1g"))

    spark.sparkContext.setLogLevel("WARN")
    is_debug = args.contains("debug")
    logger.setLevel(if (is_debug) Level.DEBUG else Level.INFO)
    // Display the configuration
    spark.conf.getAll.foreach(x => println(s"${x._1}: ${x._2}"))

    args.lift(0) match {
      case Some("QUERY")   => query(args.drop(1))
      case Some("CONVERT") => convert(args.drop(1))
      case _ => {
        logger.error(s"Usage: QUERY|CONVERT")
        System.exit(1)
      }
    }
  }

  def query(args: Array[String]) {
    val bloom = args.contains("bloom")
    logger.info(s"QUERY bloom=$bloom")
    val query = if (bloom) new Q3_Bloom else new Q3_SQL
    query.run()
  }

  def convert(args: Array[String]) {
    spark.conf.set("spark.eventLog.enabled", "false")
    val converter = new Converter(args(0))
    args.drop(1).foreach(converter.convert)
  }

  def getMaxMemory(): Long =
    Seq("spark.driver.maxResultSize", "spark.driver.memory")
      .flatMap(spark.conf.getOption)
      .map(JavaUtils.byteStringAsBytes)
      .filter(_ > 0)
      .lift(0)
      .getOrElse(1024*1024)
}
