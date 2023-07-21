package alluxio.underfs;

import alluxio.resource.ResourcePool;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A simple byte buffer resource pool. Byte buffer can be replaced with netty ByteBuf in the future.
 */
public class ByteBufferResourcePool extends ResourcePool<ByteBuffer> {
  private final int mBufferSize;

  /**
   * Creates an instance.
   * @param maxCapacity the max capacity
   * @param bufferSize the buffer size
   */
  public ByteBufferResourcePool(int maxCapacity, int bufferSize) {
    super(maxCapacity);
    mBufferSize = bufferSize;
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  @Override
  public void release(ByteBuffer resource) {
    resource.clear();
    super.release(resource);
  }

  @Override
  public ByteBuffer createNewResource() {
    // TODO: should we consider using direct memory?
    return ByteBuffer.allocate(mBufferSize);
  }
}
