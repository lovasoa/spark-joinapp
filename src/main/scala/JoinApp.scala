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

  private val appConfigParser =
    new scopt.OptionParser[AppConfig]("joinapp") {
    head("joinapp", "0.1")

    opt[Unit]('q', "query").action( (_, c) =>
      c.copy(query = true) ).text("execute the query")

    opt[Unit]('b', "bloom").action( (_, c) =>
      c.copy(bloom = true) ).text("use a bloom-filtered join")

    opt[Unit]("sql").action( (_, c) =>
      c.copy(bloom = false) ).text("use a simple SparkSQL query")

    opt[Double]('e', "error-rate").action( (x, c) =>
      c.copy(query=true, bloom=true, errorRate = x)
    ).text("error rate to use in bloom filters")

    opt[Unit]("debug").action( (_, c) =>
      c.copy(debug = true) ).text("print debug informations while doing the query")

    opt[Unit]('c', "convert").action( (_, c) =>
      c.copy(convert = true) ).text("convert files from CSV to parquet")

    opt[String]('s', "source-path").action( (x, c) =>
      c.copy(sourcePath = x) ).text("convert files from CSV to parquet")

    arg[String]("<table>...").unbounded().optional().action( (x, c) =>
      c.copy(tables = c.tables :+ x) ).text("tables to convert")
  }

  def main(args: Array[String]) {
    // Parse command-line
    appConfigParser.parse(args, AppConfig()) match {
      case Some(parsed) => conf = parsed
      case None => System.exit(1)
    }

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

  def query() {
    logger.info(s"QUERY bloom=${conf.bloom}")
    val query = if (conf.bloom) new Q3_Bloom else new Q3_SQL
    query.run()
  }

  def convert() {
    spark.conf.set("spark.eventLog.enabled", "false")
    val converter = new Converter(conf.sourcePath)
    conf.tables.foreach(converter.convert)
  }
}
