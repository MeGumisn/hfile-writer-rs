import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

/**
 * Prints every cell of an HFile with HBase's own reader, in exactly the format used by
 * {@code hfile dump}, so the two outputs can be diffed byte for byte.
 */
public final class DumpHFile {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("usage: DumpHFile <file>");
      System.exit(2);
    }
    Path path = new Path(args[0]);
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), true, conf);
    try {
      if (reader.getEntries() == 0) {
        System.out.println("READ_OK 0");
        return;
      }
      HFileScanner scanner = reader.getScanner(conf, false, false);
      scanner.seekTo();
      int count = 0;
      StringBuilder line = new StringBuilder();
      do {
        Cell cell = scanner.getCell();
        count++;
        line.setLength(0);
        line.append(hex(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())).append('/');
        line.append(hex(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
            .append('/');
        line.append(hex(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength()));
        line.append(" ts=").append(cell.getTimestamp());
        line.append(" type=").append(cell.getTypeByte() & 0xff);
        line.append(" seq=").append(cell.getSequenceId());
        line.append(" value=")
            .append(hex(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        System.out.println(line);
      } while (scanner.next());
      System.out.println("READ_OK " + count);
    } finally {
      reader.close();
    }
  }

  private static String hex(byte[] bytes, int offset, int length) {
    StringBuilder sb = new StringBuilder(length * 2);
    for (int i = 0; i < length; i++) {
      sb.append(String.format("%02x", bytes[offset + i]));
    }
    return sb.toString();
  }
}
