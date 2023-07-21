package alluxio.underfs;

import alluxio.resource.Pool;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * The multipart uploader interface to support multipart uploading.
 * The interface is inspired by hadoop {@link org.apache.hadoop.fs.impl.FileSystemMultipartUploader}
 */
public interface MultipartUploader {
  /**
   * Initialize a multipart upload.
   * @throws IOException IO failure
   */
  void startUpload() throws IOException;

  /**
   * Put part as part of a multipart upload.
   * It is possible to have parts uploaded in any order (or in parallel).
   * stream after reading in the data.
   * @param b the byte array to put. The byte buffer must have been flipped and be ready to read
   * @param partNumber the part number of this file part
   * @return a future of the async upload task
   * @throws IOException IO failure
   */
  ListenableFuture<Void> putPart(ByteBuffer b, int partNumber)
      throws IOException;

  /**
   * Complete a multipart upload.
   * @throws IOException IO failure
   */
  void complete() throws IOException;

  /**
   * Aborts a multipart upload.
   * @throws IOException IO failure
   */
  void abort() throws IOException;

  /**
   * Wait for the ongoing uploads to complete.
   * @throws IOException IO failure
   */
  void flush() throws IOException;
}
