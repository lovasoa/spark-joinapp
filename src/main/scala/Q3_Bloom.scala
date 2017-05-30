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
    val filteredOrders = spark.sql("""
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
    println(s"BloomFilter($count, $bits)")
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
      .createOrReplaceTempView("filteredLineitem")

    spark.sql("""
      SELECT
          o_orderkey,
          l_extendedprice,
          o_orderdate
      FROM
          filteredOrders,
          filteredLineitem
      WHERE
          l_orderkey = o_orderkey
    """)
  }

  def bloomSize(elements:Long, errorRate:Double) : Long = {
    // Compute the desired size of the bloom filter
    // Classic Bloom filters use 1.44 * log2(1/ϵ) bits of space per inserted key
    val requiredBits = math.round(elements * 1.44 * math.log(1/errorRate)/math.log(2))
    math.min(requiredBits, Main.getMaxMemory * 8 / 2) // Don’t use more memory than available
  }
}
