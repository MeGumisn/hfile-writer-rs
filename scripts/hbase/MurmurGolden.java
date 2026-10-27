import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Hash;

/**
 * Prints HBase's murmur2 values for a fixed set of keys and seeds.
 *
 * <p>
 * The Rust port in {@code src/hbase/bloom.rs} must produce exactly these numbers; they are
 * asserted by {@code bloom::tests::murmur_matches_hbase}. Run:
 *
 * <pre>
 * javac -cp "$HBASE_CLASSPATH" -d /tmp/classes scripts/hbase/MurmurGolden.java
 * java -cp "/tmp/classes:$HBASE_CLASSPATH" MurmurGolden
 * </pre>
 */
public final class MurmurGolden {

  /** High bit set and an odd tail length, which is where HBase differs from the C original. */
  private static final byte[] HIGH_BITS = { (byte) 0x80, (byte) 0x81, (byte) 0x82, (byte) 0x83,
    (byte) 0x84 };

  public static void main(String[] args) {
    byte[][] keys = { new byte[0], "row000000".getBytes(), "hello".getBytes(),
      "0123456789abcdef".getBytes(), HIGH_BITS,
      // The final partial word, for each tail length, with and without the high bit: Java's
      // `byte` is signed, so HBase shifts the sign extension into the hash.
      { (byte) 0x84 }, { (byte) 0x04 },
      { 0x00, (byte) 0x84 }, { (byte) 0x84, 0x00 }, { 0x00, 0x04 }, { 0x04, 0x00 },
      { 0x00, 0x00, (byte) 0x84 }, { (byte) 0x84, (byte) 0x83, (byte) 0x82 }, { 0x00, 0x00, 0x04 },
      { 0x01, 0x02, 0x03, (byte) 0x84 }, { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
      { 0x01, 0x02, 0x03, 0x04, (byte) 0x85, 0x06 }, { (byte) 0xaa, (byte) 0xbb, (byte) 0xcc,
        (byte) 0xdd, (byte) 0xee } };
    int[] seeds = { 0, 1, 0x5bd1e995, 7 };
    Hash hash = Hash.getInstance(Hash.MURMUR_HASH);
    for (byte[] key : keys) {
      for (int seed : seeds) {
        int value = hash.hash(new ByteArrayHashKey(key, 0, key.length), seed);
        System.out.println(toHex(key) + " seed=" + seed + " -> " + value);
      }
    }
  }

  private static String toHex(byte[] key) {
    StringBuilder sb = new StringBuilder();
    for (byte b : key) {
      sb.append(String.format("%02x", b));
    }
    return sb.length() == 0 ? "<empty>" : sb.toString();
  }
}
