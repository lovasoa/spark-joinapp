import org.apache.spark.sql._
import Main.spark.implicits._

class Q3_Bloom extends Q3 {
  /**
    Implement a truncated query Q3 from TPCH
    Using a prefiltering with a bloom filter
    (with only 1 join and to aggregation)
  **/
  override val queryType = "BloomFiltered"

  override def query() : DataFrame = {
    val filteredOrders = Main.spark.sql("""
      SELECT
          o_orderkey,
          o_orderdate
      FROM
          orders
      WHERE
          o_custkey % 5 = 0 -- selectivity: 1/5
          AND o_orderdate < '1995-03-15' -- selectivity: 0.48
    """)
    filteredOrders.createOrReplaceTempView("filteredOrders")

    // Create our bloom filter
    val count = filteredOrders.count().toInt
    val bits = bloomSize(count, 0.05).toInt
    println("BloomFilter($count, $bits)")
    val bloomAggregator = new BloomFilterAggregator[Int](count, bits)
    val bloomFilter =
      filteredOrders
        .select($"o_orderkey")
        .as[Int]
        .select(bloomAggregator.toColumn)
        .collect()

    // Broadcast it to all node
    val broadcastedFilter = Main.sc.broadcast(bloomFilter)

    // Filter lineitem using our bloom filter
    Main.spark.udf.register("checkInFilter", (id: Long) => broadcastedFilter.value.contains(id))
    Main.spark.sql("""
      SELECT
          o_orderkey,
          l_extendedprice
          o_orderdate
      FROM
          filteredOrders,
          lineitem
      WHERE
          AND l_orderkey = o_orderkey
          AND checkInFilter(l_orderkey)
          AND l_shipdate > '1995-03-15' -- selectivity: 0.54
    """)
  }

  def bloomSize(elements:Long, errorRate:Double) : Long = {
    // Compute the desired size of the bloom filter
    // Classic Bloom filters use 1.44 * log2(1/ϵ) bits of space per inserted key
    val requiredBits = math.round(elements * 1.44 * math.log(1/errorRate)/math.log(2))
    math.max(requiredBits, Main.getMaxMemory * 8 / 2) // Don’t use more memory than available
  }
}
