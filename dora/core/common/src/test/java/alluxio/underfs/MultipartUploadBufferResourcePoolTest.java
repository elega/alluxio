package alluxio.underfs;

import static org.junit.Assert.*;

import alluxio.resource.PooledResource;
import alluxio.underfs.ByteBufferResourcePool;
import alluxio.underfs.MultipartUploadBufferResourcePool;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultipartUploadBufferResourcePoolTest {
  @Test
  public void testBasic() throws IOException, TimeoutException {
    ByteBufferResourcePool sharedPool = new ByteBufferResourcePool(4, 1);
    MultipartUploadBufferResourcePool pool1 = new MultipartUploadBufferResourcePool(2, sharedPool);
    MultipartUploadBufferResourcePool pool2 = new MultipartUploadBufferResourcePool(2, sharedPool);
    ByteBuffer r1 = pool1.acquire();
    ByteBuffer r2 = pool1.acquire();
    ByteBuffer r3 = pool2.acquire();
    ByteBuffer r4 = pool2.acquire();
    assertNotNull(r1);
    assertNotNull(r2);
    assertNotNull(r3);
    assertNotNull(r4);
    assertNull(pool1.acquire(0, null));
    assertNull(pool2.acquire(0, null));
    pool1.release(r1);
    assertNull(pool2.acquire(0, null));
    assertNotNull(pool1.acquire(0, null));
    assertEquals(2, pool1.size());
    assertEquals(2, pool1.size());
  }

  @Test
  public void testTryAcquireLocalPoolFull()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ByteBufferResourcePool sharedPool = new ByteBufferResourcePool(2, 1);
    MultipartUploadBufferResourcePool pool1 = new MultipartUploadBufferResourcePool(1, sharedPool);
    MultipartUploadBufferResourcePool pool2 = new MultipartUploadBufferResourcePool(1, sharedPool);
    PooledResource<ByteBuffer> r1 = pool1.acquireCloseable();
    PooledResource<ByteBuffer> r2 = pool2.acquireCloseable();
    CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
      CommonUtils.sleepMs(500);
      r1.close();
    });
    assertNull(pool1.acquire(300, TimeUnit.MILLISECONDS));
    assertNotNull(pool1.acquire(300, TimeUnit.MILLISECONDS));
    f.get();
  }

  @Test
  public void testTryAcquireSharedPoolFull() throws IOException, TimeoutException {
    ByteBufferResourcePool sharedPool = new ByteBufferResourcePool(1, 1);
    MultipartUploadBufferResourcePool pool1 = new MultipartUploadBufferResourcePool(1, sharedPool);
    MultipartUploadBufferResourcePool pool2 = new MultipartUploadBufferResourcePool(1, sharedPool);
    PooledResource<ByteBuffer> r2 = pool2.acquireCloseable();
    CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
      CommonUtils.sleepMs(500);
      r2.close();
    });
    assertNull(pool1.acquire(300, TimeUnit.MILLISECONDS));
    assertNotNull(pool1.acquire(300, TimeUnit.MILLISECONDS));
  }
}