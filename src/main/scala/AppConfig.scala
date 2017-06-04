case class AppConfig(
  convert: Boolean = false,
  query: Boolean = true,
  bloom: Boolean = false,
  errorRate: Double = 0.05,
  sourcePath: String = "",
  tables: Seq[String] = Seq(),
  debug: Boolean = false
)
