import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, BasicConfigurator}

object Main {
  val conf = new SparkConf()
      .setMaster("yarn")
      .setAppName("JoinApp")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.eventLog.enabled", "true")
  conf.registerKryoClasses(Array(classOf[BloomFilter]))

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().getOrCreate()

  val logger = Logger.getLogger(Main.getClass)

  def main(args: Array[String]) {
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
    sc.setLogLevel("WARN")
    val bloom = args.contains("bloom")
    val is_debug = args.contains("debug")
    logger.info(s"QUERY bloom=$bloom")
    val query = if (bloom) new Q3_Bloom else new Q3_SQL
    query.run(debug = is_debug)
  }

  def convert(args: Array[String]) {
    sc.setLogLevel("ERROR")
    conf.set("spark.eventLog.enabled", "false")
    val converter = new Converter(args(0))
    args.drop(1).foreach(converter.convert)
  }

  def getMaxMemory(): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
