import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Writes an HFile carrying two meta blocks with HBase's own writer, so the Rust reader can be
 * compared against a meta index this project never produced.
 *
 * <pre>
 * WriteMetaHFile &lt;file&gt; &lt;rows&gt;
 * </pre>
 */
public final class WriteMetaHFile {

  /** Payloads are compared verbatim, so keep the format trivial: the raw bytes. */
  static final class RawWritable implements Writable {
    private byte[] value;

    RawWritable(byte[] value) {
      this.value = value;
    }

    byte[] value() {
      return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.write(value);
    }

    @Override
    public void readFields(DataInput in) {
      throw new UnsupportedOperationException();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: WriteMetaHFile <file> <rows>");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    int rows = Integer.parseInt(args[1]);
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    HFileContext context = new HFileContextBuilder().withCompression(Compression.Algorithm.NONE)
      .withIncludesMvcc(true).withIncludesTags(false).withBlockSize(16 * 1024).build();
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, path)
      .withFileContext(context).create();
    try {
      for (int i = 0; i < rows; i++) {
        String row = String.format("row%06d", i);
        for (int q = 0; q < 3; q++) {
          KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("f"),
            Bytes.toBytes("q" + q), 1000000L + i, KeyValue.Type.Put,
            Bytes.toBytes("value-" + row + "-q" + q));
          kv.setSequenceId(i + 1);
          writer.append(kv);
        }
      }
      writer.appendMetaBlock("meta-a", new RawWritable(Bytes.toBytes("hello-meta")));
      writer.appendMetaBlock("meta-b", new RawWritable(Bytes.toBytes("second block")));
    } finally {
      writer.close();
    }
    System.out.println("wrote " + path + " rows=" + rows + " meta=[meta-a, meta-b]");
  }
}
