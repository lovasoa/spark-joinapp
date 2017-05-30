import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import scala.util.Random

class BloomFilterAggregator(elements:Int, bits:Int)
  extends Aggregator[Int, BitSetView, BloomFilter] {

  private val numHashFunctions : Int =
    math.max(1, math.round(math.log(2).toFloat * bits / elements))

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  // TODO : set spark.kryoserializer.buffer.max if bits is too large
  def zero: BitSetView = BitSetView.ofSize(bits)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(bitset: BitSetView, value: Int): BitSetView = {
    val prng = new Random(value)
    val maxIdx = bitset.bytes.length
    for (i <- 1 to numHashFunctions) bitset.set(prng.nextInt(maxIdx))
    bitset
  }
  // Merge two intermediate values
  def merge(b1: BitSetView, b2: BitSetView): BitSetView = {
    b1.or(b2)
    b1
  }
  // Transform the output of the reduction
  def finish(bitset: BitSetView): BloomFilter =
    BloomFilter(numHashFunctions, bitset)

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[BitSetView] = Encoders.product[BitSetView]
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[BloomFilter] = Encoders.product[BloomFilter]
}

case class BloomFilter(numHashFunctions: Int, bitset: BitSetView) {
  def contains(i: Int) :  Boolean = {
    val prng = new Random(i)
    val maxIdx = bitset.bytes.length
    (1 to numHashFunctions).forall(x => bitset.get(prng.nextInt(maxIdx)))
  }
}
