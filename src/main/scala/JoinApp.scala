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
  }
}
