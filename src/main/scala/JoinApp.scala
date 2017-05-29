import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import com.github.lovasoa.bloomfilter.BloomFilter


object Main {
  val conf = new SparkConf()
      .setMaster("yarn")
      .setAppName("JoinApp")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[BloomFilter]))

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().getOrCreate()

  val folderName = "dataset/" // Folder where the data tables are

  def main(args: Array[String]) {
    sc.setLogLevel("INFO")
    val query = if (args.contains("bloom")) new Q3_Bloom else new Q3
    query.run()
  }

  def getMaxMemory(): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
