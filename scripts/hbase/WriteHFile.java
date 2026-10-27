import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Writes a deterministic data set with HBase's own writer, so the cross check has files this
 * project never touched.
 *
 * <pre>
 * WriteHFile &lt;file&gt; &lt;encoding&gt; &lt;mvcc&gt; &lt;tags&gt; &lt;rows&gt; &lt;codec&gt;
 * </pre>
 */
public final class WriteHFile {

  public static void main(String[] args) throws Exception {
    if (args.length != 6) {
      System.err.println("usage: WriteHFile <file> <encoding> <mvcc> <tags> <rows> <codec>");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    DataBlockEncoding encoding = DataBlockEncoding.valueOf(args[1]);
    boolean mvcc = Boolean.parseBoolean(args[2]);
    boolean tags = Boolean.parseBoolean(args[3]);
    int rows = Integer.parseInt(args[4]);
    Compression.Algorithm codec = Compression.Algorithm.valueOf(args[5]);

    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    HFileContext context = new HFileContextBuilder()
        .withCompression(codec)
        .withDataBlockEncoding(encoding)
        .withIncludesMvcc(mvcc)
        .withIncludesTags(tags)
        .withBlockSize(16 * 1024)
        .build();

    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path)
        .withFileContext(context)
        .create();
    try {
      for (int i = 0; i < rows; i++) {
        String row = String.format("row%06d", i);
        for (int q = 0; q < 3; q++) {
          String qualifier = "q" + q;
          byte[] value = Bytes.toBytes("value-" + row + "-" + qualifier);
          KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("f"),
              Bytes.toBytes(qualifier), 1000000L + i, KeyValue.Type.Put, value);
          if (mvcc) {
            kv.setSequenceId(i + 1);
          }
          writer.append(kv);
        }
      }
    } finally {
      writer.close();
    }
    System.out.println("wrote " + path + " encoding=" + encoding + " mvcc=" + mvcc
        + " tags=" + tags + " codec=" + codec);
  }
}
