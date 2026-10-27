import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reads a meta block from an HFile with HBase's reader, so a meta block written by this project can
 * be checked byte for byte.
 *
 * <pre>
 * MetaBlockCheck &lt;file&gt; &lt;name&gt;...
 * </pre>
 *
 * Prints `&lt;name&gt; &lt;payload&gt;` per block, or `&lt;name&gt; MISSING`.
 */
public final class MetaBlockCheck {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: MetaBlockCheck <file> <name>...");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), true, conf);
    try {
      for (int i = 1; i < args.length; i++) {
        HFileBlock block = reader.getMetaBlock(args[i], false);
        if (block == null) {
          System.out.println(args[i] + " MISSING");
          continue;
        }
        ByteBuff buffer = block.getBufferWithoutHeader();
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        System.out.println(args[i] + " " + Bytes.toString(payload));
      }
      System.out.println("metaIndexCount " + reader.getTrailer().getMetaIndexCount());
    } finally {
      reader.close();
    }
  }
}
