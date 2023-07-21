package alluxio.underfs;

import alluxio.resource.Pool;
import alluxio.resource.ResourcePool;
import alluxio.util.CommonUtils;


import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The resource pool for multipart upload. Each stream should have its dedicated resource pool.
 * There are two dimensions to control the concurrency.
 * 1. The shared resource pool limits the overall number of memory buffers in total.
 * 2. This resource pool limits the number of memory buffers the current stream can use.
 * Both are configurable.
 */
public class MultipartUploadBufferResourcePool implements Pool<ByteBuffer> {
  private final ResourcePool<ByteBuffer> mSharedResourcePool;
  private final int mLocalCapacity;
  private final Semaphore mSemaphore;

  /**
   * Creates an instance.
   * @param localCapacity the # of memory buffers this output stream can use
   * @param sharedResourcePool the shared memory buffer pool
   */
  public MultipartUploadBufferResourcePool(
      int localCapacity, ResourcePool<ByteBuffer> sharedResourcePool) {
    mLocalCapacity = localCapacity;
    mSemaphore = new Semaphore(mLocalCapacity);
    mSharedResourcePool = sharedResourcePool;
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  @Override
  public ByteBuffer acquire() throws IOException {
    try {
      mSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    try {
      return mSharedResourcePool.acquire();
    } catch (Throwable t) {
      mSemaphore.release();
      throw t;
    }
  }

  @Override
  public ByteBuffer acquire(long time, TimeUnit unit) throws TimeoutException, IOException {
    Preconditions.checkState(time >= 0, "Timeout must be no less than 0.");
    // If either time or unit are null, the other should also be null.
    Preconditions.checkState((time <= 0) == (unit == null));

    long endTimeMs = CommonUtils.getCurrentMs();
    if (unit != null) {
      endTimeMs += unit.toMillis(time);
    }
    final boolean acquired;
    try {
      if (time == 0) {
        acquired = mSemaphore.tryAcquire();
      } else {
        acquired = mSemaphore.tryAcquire(time, unit);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    if (!acquired) {
      return null;
    }
    long timeout = endTimeMs - System.currentTimeMillis();
    if (timeout < 0) {
      timeout = 0;
    }
    try {
      ByteBuffer result =
          mSharedResourcePool.acquire(timeout, timeout == 0 ? null : TimeUnit.MILLISECONDS);
      if (result == null) {
        mSemaphore.release();
      }
      return result;
    } catch (Throwable t) {
      mSemaphore.release();
      throw t;
    }
  }

  @Override
  public void release(ByteBuffer resource) {
    mSharedResourcePool.release(resource);
    mSemaphore.release();
  }

  @Override
  public int size() {
    return mLocalCapacity - mSemaphore.availablePermits();
  }
}
