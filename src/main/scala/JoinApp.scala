import org.apache.spark.sql._
import org.apache.spark.SparkContext

object Main {
  val spark = SparkSession.builder()
      .master("yarn")
      .appName("JoinApp")
      .getOrCreate()
  import spark.implicits._

  val sc = SparkContext.getOrCreate()

  def main(args: Array[String]) {
    sc.setLogLevel("INFO")

    val converter = new Converter(spark, "dataset/")
    val List(orders, lineitems) =
      List("orders", "lineitems")
        .map(TPCHTables.byName.get).flatten
        .map(converter.read)

    orders.join(lineitems, $"o_orderkey" === $"l_orderkey")
  }
}
