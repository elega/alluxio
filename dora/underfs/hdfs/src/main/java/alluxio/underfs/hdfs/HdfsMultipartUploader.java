package alluxio.underfs.hdfs;

import alluxio.exception.status.CanceledException;
import alluxio.underfs.MultipartUploader;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The multipart uploader implementation for HDFS.
 */
@NotThreadSafe
public class HdfsMultipartUploader implements MultipartUploader {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUnderFileSystem.class);

  private class UploadTask {
    private final int mPartNumber;
    private final Path mPath;
    private final long mStartedAt;
    private final int mSize;

    public UploadTask(int partNumber, Path path, int size) {
      mPartNumber = partNumber;
      mPath = path;
      mSize = size;
      mStartedAt = CommonUtils.getCurrentMs();
    }
  }

  private final FileSystem mHdfs;
  private final Map<UploadTask, ListenableFuture<Void>> mUploadMap = new HashMap<>();
  private final String mFilePath;
  private final String mTmpDirPath;
  private final ListeningExecutorService mExecutor;

  private final CreateOptions mOptions;
  private final HdfsAclProvider mHdfsAclProvider;

  /**
   * .
   * @param hdfs the file system
   * @param path the path
   * @param options the create options
   * @param executor the executor
   * @param hdfsAclProvider the acl provider
   */
  public HdfsMultipartUploader(
      FileSystem hdfs, String path,
      CreateOptions options, ExecutorService executor, HdfsAclProvider hdfsAclProvider) {
    mHdfs = hdfs;
    mExecutor = MoreExecutors.listeningDecorator(executor);
    mOptions = options;
    mHdfsAclProvider = hdfsAclProvider;
    mFilePath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    mTmpDirPath = path + "_mpu_" + UUID.randomUUID().toString() + "/";
  }

  @Override
  public void startUpload()
      throws IOException {
    mHdfs.mkdirs(new Path(mTmpDirPath));
    LOG.debug("HDFS multipart upload started for {}, temp dir path {}", mFilePath, mTmpDirPath);
  }

  @Override
  public ListenableFuture<Void> putPart(
      ByteBuffer b,
      int partNumber)
      throws IOException {
    final Path path = new Path(PathUtils.concatPath(mTmpDirPath, partNumber));
    final int uploadSize = b.limit();
    ListenableFuture<Void> f = mExecutor.submit(() -> {
      try {
        LOG.debug("Upload task initiated for {}", path);
        FSDataOutputStream os = mHdfs.create(path);
        os.write(b.array(), 0, uploadSize);
        os.close();
        LOG.debug("Upload task finished for {}", path);
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    mUploadMap.put(new UploadTask(partNumber, path, uploadSize), f);
    return f;
  }

  @Override
  public void complete() throws IOException {
    try {
      if (mUploadMap.size() == 0) {
        FileSystem.create(mHdfs, new Path(mFilePath),
            new FsPermission(mOptions.getMode().toShort())).close();
        if (mOptions.getAcl() != null) {
          mHdfsAclProvider.setAclEntries(mHdfs, mFilePath,
              mOptions.getAcl().getEntries());
        }
        mHdfs.create(new Path(mFilePath)).close();
        LOG.debug("HDFS multipart upload finished for {}", mFilePath);
        return;
      }

      doFlush();
      Path tmpFilePath = new Path(PathUtils.concatPath(mTmpDirPath, new Path(mFilePath).getName()));
      FileSystem.create(mHdfs, tmpFilePath,
          new FsPermission(mOptions.getMode().toShort())).close();
      if (mOptions.getAcl() != null) {
        mHdfsAclProvider.setAclEntries(mHdfs, tmpFilePath.toString(),
            mOptions.getAcl().getEntries());
      }
      UploadTask[] tasks = mUploadMap.keySet().toArray(new UploadTask[0]);
      Path[] pathsToMerges = new Path[tasks.length];
      Arrays.sort(tasks, Comparator.comparingInt(it -> it.mPartNumber));
      long bytesWritten = 0;
      for (int i = 0; i < tasks.length; ++i) {
        // Sanity check if upload parts are consecutive and all exist.
        if (tasks[i].mPartNumber != i) {
          System.out.println("Part " + i + " is missing.");
          throw new FileNotFoundException(
              "HDFS multipart upload sanity check failed: part " + i + " of file "
                  + mFilePath + "wasn't uploaded.");
        }
        if (!mHdfs.exists(tasks[i].mPath)) {
          throw new FileNotFoundException(
              "HDFS multipart upload sanity check failed: part " + i + " has been uploaded but "
                  + "is missing from HDFS for file " + mFilePath);
        }
        bytesWritten += tasks[i].mSize;
        pathsToMerges[i] = tasks[i].mPath;
      }
      // Concat operation requires sources and target to be in the same directory
      mHdfs.concat(tmpFilePath, pathsToMerges);
      mHdfs.rename(tmpFilePath, new Path(mFilePath));
      FileStatus fileStatus = mHdfs.getFileStatus(new Path(mFilePath));
      if (bytesWritten != fileStatus.getLen()) {
        throw new IOException(String.format("HDFS multipart upload file might be corrupted, "
                + "expected written size: %d, actual size: %d for file %s",
            bytesWritten, fileStatus.getLen(), tmpFilePath));
      }
      LOG.debug("HDFS multipart upload finished for {}, bytes written: {}",
          mFilePath, bytesWritten);
    } finally {
      mHdfs.delete(new Path(mTmpDirPath), true);
    }
  }

  @Override
  public void flush() throws IOException {
    try {
      doFlush();
    } catch (Exception e) {
      mHdfs.delete(new Path(mTmpDirPath), true);
      throw e;
    }
  }

  private void doFlush() throws IOException {
    try {
      Futures.allAsList(mUploadMap.values()).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public void abort() throws IOException {
    Path path = new Path(mTmpDirPath);
    // force a check for a file existing; raises FNFE if not found
    mHdfs.getFileStatus(path);
    mHdfs.delete(path, true);
  }
}
