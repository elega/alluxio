/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.hdfs;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import alluxio.underfs.ContentHashable;
import alluxio.underfs.MultipartUploadBufferResourcePool;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A multipart upload output stream implementation for {@link HdfsUnderFileSystem}.
 * The content to write is stored in a memory buffer. Once the bytes in the buffer reaches
 * a certain size, the stream will start uploading the file part as a temporary file asynchronously.
 * When the stream is closed, the stream will wait for all uploading tasks to complete ann then
 * concat them.
 */
@NotThreadSafe
public class HdfsUnderFileMultipartUploadOutputStream
    extends OutputStream implements ContentHashable {
  /**
   * An exception thrown when the memory buffer is full.
   */
  public static class MemoryBufferExhaustedException extends Exception {
    /**
     * .
     */
    public MemoryBufferExhaustedException() {}

    /**
     * .
     * @param e the inner exception
     */
    public MemoryBufferExhaustedException(Exception e) {
      super(e);
    }
  }

  /**
   * Underlying output stream.
   */
  private final FileSystem mFs;
  private final String mPath;
  private int mPartNumber = 0;

  private final MultipartUploadBufferResourcePool mBufferPool;
  private final HdfsMultipartUploader mMultipartUploader;
  private ByteBuffer mCurrentBuffer;

  /**
   * Creates an output stream.
   * @param fs the file system
   * @param path the file path to write
   * @param options the options
   * @param aclProvider the acl provider
   * @param sharedPool the shared resource pool
   * @throws MemoryBufferExhaustedException if the memory buffer is full
   */
  public HdfsUnderFileMultipartUploadOutputStream(
      FileSystem fs, String path, CreateOptions options, HdfsAclProvider aclProvider,
      ResourcePool<ByteBuffer> sharedPool)
      throws IOException, MemoryBufferExhaustedException {
    mBufferPool = new MultipartUploadBufferResourcePool(
        Configuration.getInt(
            PropertyKey.UNDERFS_HDFS_MULTIPART_UPLOAD_MEMORY_BUFFER_POOL_CAPACITY_PER_STREAM),
        sharedPool);
    // Secure a buffer before writing the data.
    // If no buffer is out there, can fall back to the normal output stream.
    long firstBufferAcquireTimeout = Configuration.getMs(
        PropertyKey.UNDERFS_HDFS_MULTIPART_UPLOAD_STREAM_CREATION_BUFFER_ACQUIRE_TIMEOUT_MS);
    if (firstBufferAcquireTimeout >= 0) {
      try {
        mCurrentBuffer = mBufferPool.acquire(
            firstBufferAcquireTimeout,
            firstBufferAcquireTimeout != 0 ? TimeUnit.MILLISECONDS : null
        );
      } catch (TimeoutException e) {
        throw new MemoryBufferExhaustedException(e);
      }
    }
    mFs = fs;
    mPath = path;
    // The concurrency control is done by the resource pool,
    // so here we just create a cached thread pool.
    ExecutorService executor = Executors.newCachedThreadPool();
    mMultipartUploader = new HdfsMultipartUploader(fs, path, options, executor, aclProvider);
    mMultipartUploader.startUpload();
  }

  @Override
  public void close() throws IOException {
    if (mCurrentBuffer != null && mCurrentBuffer.position() > 0) {
      uploadCurrent();
    }
    mMultipartUploader.complete();
  }

  /**
   * Flush the current ongoing upload parts into HDFS.
   * When fuse is used, before a file is closed(), the file will be flushed() first.
   * Doing a flush and persist all ongoing upload parts into HDFS helps mitigate
   * the fuse async release issue.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    mMultipartUploader.flush();
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b});
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null || len == 0) {
      return;
    }
    Preconditions.checkNotNull(b);
    Preconditions.checkArgument(off >= 0 && off <= b.length
        && len >= 0 && off + len <= b.length);

    if (mCurrentBuffer == null) {
      mCurrentBuffer = mBufferPool.acquire();
    }

    int lenOfCurrentBuffer = mCurrentBuffer.position() + len <= mCurrentBuffer.capacity()
        ? len : mCurrentBuffer.limit() - mCurrentBuffer.position();
    mCurrentBuffer.put(b, off, lenOfCurrentBuffer);
    if (!mCurrentBuffer.hasRemaining()) {
      uploadCurrent();
    }

    if (mCurrentBuffer == null) {
      mCurrentBuffer = mBufferPool.acquire();
    }
    int lenRemaining = len - lenOfCurrentBuffer;
    if (lenRemaining > 0) {
      write(b, off + lenOfCurrentBuffer, len - lenOfCurrentBuffer);
    }
  }

  private void uploadCurrent() throws IOException {
    mCurrentBuffer.flip();
    ListenableFuture<Void> f = mMultipartUploader.putPart(mCurrentBuffer, mPartNumber++);
    final ByteBuffer bufferToRelease = mCurrentBuffer;
    mCurrentBuffer = null;
    Futures.addCallback(f, new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void result) {
        mBufferPool.release(bufferToRelease);
      }

      @Override
      public void onFailure(Throwable t) {
        mBufferPool.release(bufferToRelease);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public Optional<String> getContentHash() throws IOException {
    // TODO: can calculate the HDFS checksum on the fly
    FileStatus fs = mFs.getFileStatus(new Path(mPath));
    // get the content hash immediately after the file has completed writing
    // which will be used for generating the fingerprint of the file in Alluxio
    // ideally this value would be received as a result from the close call
    // so that we would be sure to have the hash relating to the file uploaded
    // (but such an API is not available for HDFS)
    return Optional.of(UnderFileSystemUtils.approximateContentHash(
        fs.getLen(), fs.getModificationTime()));
  }
}
