import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.util.sketch.BloomFilter
import Main.{spark, logger, sc}

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

    // Getting an fast approximation of the number of distinct order keys
    sc.setJobGroup("countApprox", "Estimating the number of elements in the filtered small table")
    var countedParts = 0L
    var countedElements = 0L
    var allParts = filteredOrders.rdd.getNumPartitions.toLong
    sc.runJob(
      filteredOrders.rdd,
      (it:Iterator[_]) => it.size,
      (_, partCount:Int) => {
        countedParts += 1
        countedElements += partCount.toLong
        if (countedParts > 0.1 * allParts) {
          sc.cancelJobGroup("countApprox")
        }
      }
    )
    val count : Int = (countedElements * allParts / countedParts).toInt
    logger.info(s"Counted $countedElements in $countedParts ($allParts total). Estimating $count elements")

    // Create our bloom filter
    val errorRate = Main.conf.errorRate
    logger.info(f"BloomFilter($count elements, ${errorRate * 100}%.2f %% error rate)")
    val bloomFilter : BloomFilter = TreeBloom.bloomFilter(
        singleCol = filteredOrders.select($"o_orderkey"),
        expectedNumItems = count,
        fpp = errorRate)

    logger.info(s"BloomFilter size: ${bloomFilter.bitSize} bits")

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
