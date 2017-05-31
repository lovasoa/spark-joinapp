import org.apache.spark.sql._
import Main.spark.implicits._
import Main.{spark, logger}

abstract class Q3() {
  /**
    Implement a truncated query Q3 from TPCH
    (with only 1 join and to aggregation)
  **/
  val queryType = "Simple"

  def registerView(tableName: String) = {
    val file = Converter.parquetFileName(tableName)
    spark.read.parquet(file).createOrReplaceTempView(tableName)
  }

  def prepare() = {
    List("orders", "lineitem").foreach(registerView)
  }

  // Concrete Q3 must override this
  def query() : DataFrame

  def run_debug() = {
    prepare()
    logger.info(s"query type: $queryType")
    spark.sql("""
      SELECT 'orders' AS table, COUNT(*) AS count FROM orders
      UNION
      SELECT 'lineitem' AS table, COUNT(*) AS count FROM lineitem
    """).show()
    val result = query()
    result.explain()
    result.show()
    result.write.mode(SaveMode.Overwrite).parquet("Q3-result.parquet")
    logger.info(s"number of elements in result set: ${result.count()}")
    result
  }

  def run() = {
    prepare()
    logger.info(s"query type: $queryType")
    val result = query()
    result.write.mode(SaveMode.Overwrite).parquet("Q3-result.parquet")
    result
  }

}

/**
## The real Q3
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING' -- selectivity: 1/5
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15' -- selectivity: 0.48
    AND l_shipdate > date '1995-03-15' -- selectivity: 0.54
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue desc,
    o_orderdate
LIMIT 20;

## The simplified Q3 we implement
SELECT
    l_orderkey,
    l_extendedprice
    o_orderdate
FROM
    order,
    lineitem
WHERE
    o_custkey % 5 = 0 -- selectivity: 1/5
    AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15' -- selectivity: 0.48
    AND l_shipdate > '1995-03-15' -- selectivity: 0.54
**/
