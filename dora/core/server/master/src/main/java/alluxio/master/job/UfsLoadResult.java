package alluxio.master.job;

import alluxio.underfs.UfsStatus;

import java.util.List;
import java.util.stream.Stream;

public class UfsLoadResult {

  public UfsLoadResult(List<UfsStatus> items, boolean isTruncated) {
    mItems = items;
    mIsTruncated = isTruncated;
  }

  private final List<UfsStatus> mItems;
  private final boolean mIsTruncated;

  public List<UfsStatus> getItems() {
    return mItems;
  }

  public boolean isTruncated() {
    return mIsTruncated;
  }
}
