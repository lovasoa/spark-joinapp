import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.util.sketch.BloomFilter
import Main.{spark, logger, sc}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent._

class Q3_Bloom extends Q3 {
  import cspark.implicits._

  /**
    Implement a truncated query Q3 from TPCH
    Using a prefiltering with a bloom filter
    (with only 1 join and to aggregation)
  **/
  override val queryType = "BloomFiltered"

  override def query() : DataFrame = {
    val filteredOrders =
      spark.read.table("orders")
        .filter($"o_custkey" % 5 === 0 && $"o_orderdate" < "1995-03-15")
        .select($"o_orderkey", $"o_orderdate")
    filteredOrders.cache()

    var futureBloomFilter : Future[BloomFilter] = null
    // Getting an fast approximation of the number of distinct order keys
    countAsync(filteredOrders.rdd, (precision:Double, count:Int) => {
      if (futureBloomFilter == null && precision >= 0.1) {
        futureBloomFilter = makeBloomFilter(filteredOrders, count)
      }
    })
    val bloomFilter = Await.result(futureBloomFilter, Duration.Inf)
    // Broadcast it to all node
    val broadcastedFilter = sc.broadcast(bloomFilter)
    // Filter lineitem using our bloom filter
    val checkInFilter = udf((x:Long) => broadcastedFilter.value.mightContainLong(x))
    val lineitem = spark.read.table("lineitem")

    if (Main.conf.debug) debug(lineitem, checkInFilter)

    sc.setJobGroup("join", "Joining the two tables (includes filtering the large table)")
    spark.read.table("lineitem")
      .filter($"l_shipdate" > "1995-03-15" && checkInFilter($"l_orderkey"))
      .join(filteredOrders, $"l_orderkey" === $"o_orderkey")
      .select($"o_orderkey", $"l_extendedprice", $"o_orderdate")
  }

  def countAsync(rdd:RDD[_], update:((Double, Int)=>Unit)) : Unit = {
    sc.setJobGroup("countApprox", "Estimating the number of elements in the filtered small table")
    var countedParts = 0L
    var countedElements = 0L
    var allParts = rdd.getNumPartitions.toLong
    sc.runJob(
      rdd,
      (it:Iterator[_]) => it.size,
      (_, partCount:Int) => {
        countedParts += 1
        countedElements += partCount.toLong
        val count : Int = (countedElements * allParts / countedParts).toInt
        update(countedParts.toDouble/allParts, count)
      }
    )
  }

  def makeBloomFilter(filteredOrders:DataFrame, count:Int) : Future[BloomFilter] = {
    // Create our bloom filter
    Future {
      val errorRate = Main.conf.errorRate
      logger.info(f"BloomFilter($count elements, ${errorRate * 100}%.2f %% error rate)")
      TreeBloom.bloomFilter(
          singleCol = filteredOrders.select($"o_orderkey"),
          expectedNumItems = count,
          fpp = errorRate)
    }
  }

  def debug(lineitem:DataFrame, checkInFilter:UserDefinedFunction) = {
    logger.debug(s"""Testing bloom filter:""")
    logger.debug(s""" Total: ${lineitem.count()}""")
    logger.debug(s""" In Bloom Filter: ${lineitem.filter(checkInFilter($"l_orderkey")).count()}""")

    val lines = lineitem.select($"l_orderkey")
        .distinct()
        .withColumn("inFilter", checkInFilter($"l_orderkey"))
        .show(numRows=100, truncate=false)
  }
}
