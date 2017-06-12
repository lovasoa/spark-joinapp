import Main.{spark, logger}

abstract class JoinApp {
  def run() : Unit
  def isSelected() : Boolean
  def runIfSelected = if (isSelected) run()
}

object JoinApp {
  def runAll(conf: AppConfig) {
    val actions = Seq[JoinApp](
      new ConvertJoinApp(conf, new Converter(conf)),
      new SQLQueryJoinApp(conf, new Q3_SQL()),
      new BloomQueryJoinApp(conf, new Q3_Bloom())
    )
    actions.foreach(_.runIfSelected)
  }
}

abstract class QueryJoinApp(conf: AppConfig)
  extends JoinApp {
  def query() : Q3
  def isQueryType() : Boolean
  override def run() {
    logger.info(s"QUERY bloom=${conf.bloom}")
    query.run()
  }
  override def isSelected = conf.query && isQueryType
}

class SQLQueryJoinApp(conf: AppConfig, q:Q3_SQL) extends QueryJoinApp(conf) {
  override def isQueryType = !conf.bloom
  override def query = q
}

class BloomQueryJoinApp(conf: AppConfig, q:Q3_Bloom) extends QueryJoinApp(conf) {
  override def isQueryType = conf.bloom
  override def query = q
}

class ConvertJoinApp(conf: AppConfig, converter: Converter) extends JoinApp {
  override def run() {
    spark.conf.set("spark.eventLog.enabled", "false")
    conf.tables.foreach(converter.convert)
  }
  override def isSelected = conf.convert
}
