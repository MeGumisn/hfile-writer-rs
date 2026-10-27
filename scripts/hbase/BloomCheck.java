import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Loads the bloom filter of an HFile with HBase's own code and answers membership queries, so this
 * project's filter can be compared byte for byte with what HBase expects.
 *
 * <pre>
 * BloomCheck &lt;file&gt; &lt;row&gt;...
 * </pre>
 *
 * Prints one `&lt;row&gt; maybe|no` line per row, the same format as `hfile bloom`.
 */
public final class BloomCheck {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: BloomCheck <file> <row>...");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), true, conf);
    try {
      BloomFilter filter =
        BloomFilterFactory.createFromMeta(reader.getGeneralBloomFilterMetadata(), reader);
      if (filter == null) {
        System.err.println("no bloom filter in " + path);
        System.exit(1);
      }
      for (int i = 1; i < args.length; i++) {
        // `hex:aabb` spells the binary rows that cannot be typed into a shell argument.
        byte[] row = args[i].startsWith("hex:") ? fromHex(args[i].substring(4))
          : Bytes.toBytes(args[i]);
        // A null buffer is fine: `CompoundBloomFilter.supportsAutoLoading()` reads the chunk block.
        boolean maybe = filter.contains(row, 0, row.length, null);
        System.out.println(args[i] + " " + (maybe ? "maybe" : "no"));
      }
    } finally {
      reader.close();
    }
  }

  private static byte[] fromHex(String hex) {
    if (hex.length() % 2 != 0) {
      throw new IllegalArgumentException("odd number of hex digits: " + hex);
    }
    byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }
}
