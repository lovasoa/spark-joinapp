case class AppConfig(
  convert: Boolean = false,
  query: Boolean = false,
  bloom: Boolean = false,
  errorRate: Double = 0.05,
  sourcePath: String = "",
  tables: Seq[String] = Seq(),
  debug: Boolean = false
)

object AppConfig {
  def parser =
    new scopt.OptionParser[AppConfig]("joinapp") {
    head("joinapp", "0.1")

    opt[Unit]('q', "query").action( (_, c) =>
      c.copy(query = true) ).text("execute the query")

    opt[Unit]('b', "bloom").action( (_, c) =>
      c.copy(query = true, bloom = true)
    ).text("use a bloom-filtered join")

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

  def parse(args: Array[String]) : Option[AppConfig] =
    parser.parse(args, AppConfig())

  def parseOrDie(args: Array[String]) : AppConfig =
    parse(args) match {
      case Some(parsed) => parsed
      case None => {System.exit(1); AppConfig()}
    }
}
