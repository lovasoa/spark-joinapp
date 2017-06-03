import Main.spark

class Q3_SQL extends Q3 {
  override def query() = {
    // Run the query and save the result to a parquet file in HDFS
    spark.sql("""
      SELECT
        l_orderkey,
        l_extendedprice,
        o_orderdate
      FROM
        orders,
        lineitem
      WHERE
        o_custkey % 50 = 0 -- selectivity: 1/50
        AND l_orderkey = o_orderkey
        AND o_orderdate < '1995-03-15' -- selectivity: 0.48
        AND l_shipdate > '1995-03-15' -- selectivity: 0.54
      """)
    }
  }
