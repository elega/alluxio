package alluxio.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

import java.util.concurrent.TimeUnit;

public class ShadeFriendlyManagedChannel extends ManagedChannel {
  private final ManagedChannel mChannel;

  public ShadeFriendlyManagedChannel(ManagedChannel channel) {
    mChannel = channel;
  }

  @Override
  public ManagedChannel shutdown() {
    return mChannel.shutdown();
  }

  @Override
  public boolean isShutdown() {
    return mChannel.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return mChannel.isTerminated();
  }

  @Override
  public ManagedChannel shutdownNow() {
    return mChannel.shutdownNow();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return mChannel.awaitTermination(timeout, unit);
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    MethodDescriptor<RequestT, ResponseT> transformedDescriptor = methodDescriptor.toBuilder()
        .setFullMethodName(methodDescriptor.getFullMethodName().replace("legacy.", ""))
        .build();
    return mChannel.newCall(transformedDescriptor, callOptions);
  }

  @Override
  public String authority() {
    return mChannel.authority();
  }

  public ManagedChannel getChannel() {
    return mChannel;
  }

  @Override
  public ConnectivityState getState(boolean requestConnection) {
    return mChannel.getState(requestConnection);
  }

  @Override
  public void enterIdle() {
    mChannel.enterIdle();
  }

  @Override
  public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
    mChannel.notifyWhenStateChanged(source, callback);
  }

  @Override
  public void resetConnectBackoff() {
    mChannel.resetConnectBackoff();
  }
}
