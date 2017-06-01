import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, BasicConfigurator}

object Main {
  var spark : SparkSession = null
  def sc = spark.sparkContext
  val logger = Logger.getLogger(Main.getClass)

  def main(args: Array[String]) {
    spark = SparkSession.builder()
      .master("yarn")
      .appName("JoinApp " ++ args.mkString(" "))
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()

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
    spark.sparkContext.setLogLevel("WARN")
    val bloom = args.contains("bloom")
    val is_debug = args.contains("debug")
    logger.info(s"QUERY bloom=$bloom")
    val query = if (bloom) new Q3_Bloom else new Q3_SQL
    query.run(debug = is_debug)
  }

  def convert(args: Array[String]) {
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.eventLog.enabled", "false")
    val converter = new Converter(args(0))
    args.drop(1).foreach(converter.convert)
  }

  def getMaxMemory(): Long = {
    val conf = spark.sparkContext.getConf
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
