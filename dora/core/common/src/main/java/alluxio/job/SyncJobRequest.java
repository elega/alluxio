package alluxio.job;

import alluxio.grpc.CopyJobPOptions;
import alluxio.grpc.LoadJobPOptions;
import alluxio.grpc.SyncJobPOptions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The request of loading files.
 */
@ThreadSafe
public class SyncJobRequest implements JobRequest {
  private static final String TYPE = "sync";
  private static final long serialVersionUID = -1145141919810893L;

  private final String mPath;

  public String getPath() {
    return mPath;
  }

  public SyncJobPOptions getOptions() {
    return mOptions;
  }

  private final SyncJobPOptions mOptions;

  /**
   * *
   * @param path
   * @param options
   */
  public SyncJobRequest(
      @JsonProperty("path") String path,
      @JsonProperty("syncJobPOptions") SyncJobPOptions options) {
    mPath = path;
    mOptions = options;
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
