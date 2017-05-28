import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import com.github.lovasoa.bloomfilter.BloomFilter

class BloomFilterAggregator(elements:Int, bits:Int)
  extends Aggregator[Object, BloomFilter, BloomFilter] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: BloomFilter = new BloomFilter(elements, bits)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: BloomFilter, value: Object): BloomFilter = {
    buffer.add(value)
    buffer
  }
  // Merge two intermediate values
  def merge(b1: BloomFilter, b2: BloomFilter): BloomFilter = {
    b1.merge(b2)
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: BloomFilter): BloomFilter = reduction
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[BloomFilter] = Encoders.kryo[BloomFilter]
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[BloomFilter] = bufferEncoder
}
