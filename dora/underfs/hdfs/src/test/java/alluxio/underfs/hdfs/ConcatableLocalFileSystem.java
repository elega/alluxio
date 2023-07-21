package alluxio.underfs.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class ConcatableLocalFileSystem extends LocalFileSystem {
  /*
  @Override
  public void concat(Path f, Path[] psrcs) throws IOException {
    try (FSDataOutputStream os = create(f, true)) {

      for (int i = 0; i < psrcs.length; ++i) {
        Files.readAllBytes()
      }
    }


    List<String> lines = Files.readAllLines(Paths.get(filePath));
    String result = String.join("", lines);
  }
   */
}
