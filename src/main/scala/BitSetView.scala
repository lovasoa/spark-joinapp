import Main.logger

case class BitSetView(bytes: Array[Byte]) {
  /**
    Access individual bits of a byte array
    Java bean class
    i >> 3 == i / 8 (the index of the byte containing the desired bit in our array of bytes)
    i & 7  == i % 8 (the index of the desired bit inside our byte)
  **/

  // Get the bit at index i
  def get(i: Int) : Boolean =
    (bytes(i >> 3) & (1 << (i & 7))) != 0

  // Set the bit at index i to 1
  def set(i: Int) : Unit = {
    val idx = i >> 3
    bytes(idx) = (bytes(idx).toInt | (1 << (i&7))).toByte
  }

  // Set the bit at index i to 0
  def unset(i: Int) : Unit = {
    val idx = i >> 3
    bytes(idx) = (bytes(idx) & ~(1 << (i & 7))).toByte
  }

  // Add all the bits in the other array to this bitset
  def or(other: BitSetView) : Unit = {
    var i = 0
    val l = bytes.length
    while (i < l) {
      bytes(i) = (bytes(i) | other.bytes(i)).toByte
      i += 1
    }
  }
}

object BitSetView {
    // Create a new BitSetView
    def ofSize(requiredSizeInBits: Long) : BitSetView = {
      val requiredSizeInBytes = requiredSizeInBits / 8
      if (requiredSizeInBytes > Int.MaxValue) {
        logger.warn(s"Required BitSet size $requiredSizeInBytes bytes" ++
          s"larger than max integer size ${Int.MaxValue}. Using Int.MaxValue")
      }
      val sizeInBytes = math.min(requiredSizeInBytes, Int.MaxValue).toInt
      val bytes = Array.ofDim[Byte](sizeInBytes)
      new BitSetView(bytes)
    }
}
