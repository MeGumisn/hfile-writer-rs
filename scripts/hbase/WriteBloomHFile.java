import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Writes an HFile with a ROW bloom filter using HBase's own writer, so the Rust reader can be
 * compared against a filter this project never produced.
 *
 * <pre>
 * WriteBloomHFile &lt;file&gt; &lt;rows&gt; &lt;codec&gt; [binary]
 * </pre>
 *
 * With `binary` the rows are 9 bytes -- one salt byte plus the big endian row number -- which is
 * what a real salted row key looks like. Those keys end in a high bit half the time, so they
 * exercise HBase's signed murmur tail, the place where a zero extending hash diverges.
 */
public final class WriteBloomHFile {

  /** Salt byte plus a big endian counter: an odd length, so the last byte is the tail. */
  private static byte[] binaryRow(int i) {
    byte[] row = new byte[9];
    row[0] = (byte) (i & 0xff);
    // A `long`, because `int >>> 32` in Java shifts by 32 % 32 == 0.
    long value = i;
    for (int b = 0; b < 8; b++) {
      row[8 - b] = (byte) (value >>> (8 * b));
    }
    return row;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3 && args.length != 4) {
      System.err.println("usage: WriteBloomHFile <file> <rows> <codec> [binary]");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    int rows = Integer.parseInt(args[1]);
    Compression.Algorithm codec = Compression.Algorithm.valueOf(args[2]);
    boolean binary = args.length == 4 && args[3].equals("binary");

    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext context = new HFileContextBuilder().withCompression(codec).withIncludesMvcc(true)
      .withIncludesTags(false).withBlockSize(16 * 1024).build();

    HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf).withPath(fs, path)
      .withFileContext(context).create();
    BloomFilterWriter bloom = BloomFilterFactory.createGeneralBloomAtWrite(conf, cacheConf,
      BloomType.ROW, rows, writer);
    try {
      byte[] prevRow = null;
      for (int i = 0; i < rows; i++) {
        byte[] row = binary ? binaryRow(i) : Bytes.toBytes(String.format("row%06d", i));
        for (int q = 0; q < 3; q++) {
          byte[] value = Bytes.toBytes("value-" + i + "-q" + q);
          KeyValue kv = new KeyValue(row, Bytes.toBytes("f"), Bytes.toBytes("q" + q),
            1000000L + i, KeyValue.Type.Put, value);
          kv.setSequenceId(i + 1);
          writer.append(kv);
          // One bloom entry per distinct row, exactly like `BloomContext.writeBloom`.
          if (prevRow == null || !Bytes.equals(row, prevRow)) {
            bloom.append(kv);
            prevRow = row;
          }
        }
      }
    } finally {
      // `StoreFileWriter` registers the filter only after the last cell: `addGeneralBloomFilter`
      // is a no-op while the filter still holds zero keys.
      writer.addGeneralBloomFilter(bloom);
      writer.close();
    }
    System.out.println("wrote " + path + " rows=" + rows + " codec=" + codec + " bloom=ROW");
  }
}
