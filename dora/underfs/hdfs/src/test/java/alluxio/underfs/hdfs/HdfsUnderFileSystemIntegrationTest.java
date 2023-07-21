package alluxio.underfs.hdfs;

import static junit.framework.TestCase.assertEquals;
import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsUnderFileSystemIntegrationTest extends HdfsUnderFileSystemIntegrationTestBase {
  @Test
  public void testWriteEmptyFile() throws Exception {
    writeEmptyFileTest();
  }

  @Test
  public void testWriteMultiBlockFileTest() throws Exception {
    writeMultiBlockFileTest();
  }

  @Test(expected = IOException.class)
  public void testException() throws Exception {
    hdfsDownDuringUploadTest();
  }

  // TODO add concurrent uploading case.
}
