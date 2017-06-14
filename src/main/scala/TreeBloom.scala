import org.apache.spark.util.sketch.BloomFilter
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import Main.sc

object TreeBloom {
  // Implements bloom filter using treeAggregate instead of aggregate
  // See https://issues.apache.org/jira/browse/SPARK-21039
  def bloomFilter(singleCol: DataFrame, expectedNumItems:Long, fpp:Double): BloomFilter = {
    val zero = BloomFilter.create(expectedNumItems, fpp)
    sc.setJobGroup("bloomFilter", "Bloom filter creation")
    singleCol.queryExecution.toRdd.treeAggregate(zero)(
      (filter: BloomFilter, row: InternalRow) => {
        filter.putLong(row.getInt(0))
        filter
      },
      (filter1, filter2) => filter1.mergeInPlace(filter2)
    )
  }
}
