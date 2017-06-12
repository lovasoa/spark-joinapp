import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, BasicConfigurator, Level}
import org.apache.spark.network.util.JavaUtils
import scopt._

object Main {
  var spark : SparkSession = null
  def sc = spark.sparkContext
  val logger = Logger.getLogger(Main.getClass)
  var conf : AppConfig = AppConfig()

  def main(args: Array[String]) {
    // Parse command-line
    conf = AppConfig.parseOrDie(args)

    spark = SparkSession.builder()
      .master("yarn")
      .appName("JoinApp " ++ args.mkString(" "))
      .config("spark.eventLog.enabled", "true")
      .config("spark.driver.maxResultSize", "0")
      .getOrCreate()

    spark.conf.set("spark.driver.maxResultSize",
      spark.conf.get("spark.driver.memory", "1g"))

    spark.sparkContext.setLogLevel("WARN")
    logger.setLevel(if (conf.debug) Level.DEBUG else Level.INFO)
    // Display the configuration
    spark.conf.getAll.foreach(x => println(s"${x._1}: ${x._2}"))

    if (conf.convert) convert()
    if (conf.query) query()
  }
}
