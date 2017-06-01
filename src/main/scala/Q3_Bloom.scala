import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    var cntPartial = filteredOrders.rdd.countApprox(timeout=3000, confidence=0.75)
    val interval = if (cntPartial.initialValue.confidence < 0.75) {
      logger.warn(s"Count interval with low confidence ($cntPartial). Waiting.")
      cntPartial.getFinalValue()
    } else { cntPartial.initialValue }
    val count : Int = cntPartial.initialValue.mean.toInt

    // Create our bloom filter
    val bits = bloomSizeInBits(elements=count, errorRate=0.05)
    logger.info(s"BloomFilter($count elements, $bits bits)")
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


    val lineitem = spark.read.table("lineitem")

    if (Main.is_debug) {
      logger.debug(s"""Testing bloom filter:""")
      logger.debug(s""" Total: ${lineitem.count()}""")
      logger.debug(s""" In Bloom Filter: ${lineitem.filter(checkInFilter($"l_orderkey")).count()}""")

      val bloomHashes = udf((id: Int) =>
        BloomFilter.hashes(
          bloomFilter.numHashFunctions,
          bloomFilter.bitset.size,
          id
        ).map(i => i.toString)
        .mkString(" ")
      )

      val lines = lineitem.select($"l_orderkey")
          .distinct()
          .withColumn("inFilter", checkInFilter($"l_orderkey"))
          .withColumn("hashes", bloomHashes($"l_orderkey"))
          .show(numRows=100, truncate=false)
    }

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
      logger.warn(s"""
        |Reached maximum memory size for the bloom filter." ++
        |Wanted to use $requiredBits bits, using only $maxSizeInBits
        |""".stripMargin.trim)
    }
    math.min(requiredBits, maxSizeInBits)
  }
}
