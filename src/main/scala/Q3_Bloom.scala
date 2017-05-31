import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import Main.spark.implicits._
import Main.{spark, sc, logger}

class Q3_Bloom extends Q3 {
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

    // Getting an approximation of the number of distinct order keys
    val cntInterval = filteredOrders.rdd.countApprox(timeout=3000, confidence=0.8)
    val (lowCnt,highCnt) = (cntInterval.initialValue.low, cntInterval.initialValue.high)
    val count : Int = math.round((lowCnt + highCnt).toFloat / 2)
    if (highCnt - lowCnt > 100) {
      logger.warn(s"Count interval: [$lowCnt, $highCnt]. Choosing count=$count")
    }

    // Create our bloom filter
    val bits = bloomSizeInBits(elements=count, errorRate=0.1)
    logger.info(s"BloomFilter($count, $bits)")
    val bloomAggregator = new BloomFilterAggregator(count, bits)
    val bloomFilter : BloomFilter =
      filteredOrders
        .select($"o_orderkey")
        .as[Int]
        .select(bloomAggregator.toColumn)
        .first()

    // Broadcast it to all node
    val broadcastedFilter = sc.broadcast(bloomFilter)

    // Filter lineitem using our bloom filter
    val checkInFilter = udf((id: Int) => broadcastedFilter.value.contains(id))

    spark.read.table("lineitem")
      .filter($"l_shipdate" > "1995-03-15" && checkInFilter($"l_orderkey"))
      .join(filteredOrders, $"l_orderkey" === $"o_orderkey")
      .select($"o_orderkey", $"l_extendedprice", $"o_orderdate")
  }

  def bloomSizeInBits(elements:Long, errorRate:Double) : Long = {
    // Compute the desired size of the bloom filter
    // Classic Bloom filters use 1.44 * log2(1/ϵ) bits of space per inserted key
    val requiredBits = math.round(elements * 1.44 * math.log(1/errorRate)/math.log(2))
    val maxMemoryFraction = 0.25 // Don’t use more memory than this percentage of total available memory
    val maxSizeInBits = (Main.getMaxMemory * 8 * maxMemoryFraction).toLong
    if (requiredBits > maxSizeInBits) {
      logger.warn("Reached maximum memory size for the bloom filter." ++
        s"Wanted to use $requiredBits bits, using only $maxSizeInBits")
    }
    math.min(requiredBits, maxSizeInBits)
  }
}
