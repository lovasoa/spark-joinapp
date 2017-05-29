import org.apache.spark.sql._
import org.apache.spark.SparkContext

object Main {
  val spark = SparkSession.builder()
      .master("yarn")
      .appName("JoinApp")
      .getOrCreate()
  val sc = SparkContext.getOrCreate()
  val folderName = "dataset/" // Folder where the data tables are

  def main(args: Array[String]) {
    sc.setLogLevel("INFO")

    val query = if (args.contains("bloom")) new Q3_Bloom else new Q3
    query.run()
  }

  def getMaxMemory(): Long = {
    val conf = sc.getConf
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
