// Query Q3 from TPC-H

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

import Main.spark.implicits._

object Q3 {
  def registerView(tableName: String) = {
    Converter.read(tableName).createOrReplaceTempView(tableName)
  }

  def query() : DataFrame {
    List("orders", "lineitem").foreach(registerView)

    val result = Main.spark.sql("""
      SELECT
          l_orderkey,
          l_extendedprice
          o_orderdate
      FROM
          orders,
          lineitem
      WHERE
          o_custkey % 5 = 0 -- selectivity: 1/5
          AND l_orderkey = o_orderkey
          AND o_orderdate < '1995-03-15' -- selectivity: 0.48
          AND l_shipdate > '1995-03-15' -- selectivity: 0.54
    """)
    result.explain()
    result
  }
}
