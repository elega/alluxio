package alluxio.master.job;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.annotation.Nullable;

public class NaiveSyncHandler {
  MountTable mMountTable;
  private boolean mCancelled;

  public NaiveSyncHandler(DefaultFileSystemMaster fileSystemMaster) {
    mMountTable = fileSystemMaster.getMountTable();
  }

  public void cancel() {
    mCancelled = true;
  }

  public void fetchUfsStatus(
      AlluxioURI path, boolean recursive, @Nullable String startFrom,
      Consumer<UfsLoadResult> onBatchFetched,
      Consumer<Exception> onFetchFailed
  ) throws IOException, InvalidPathException {
    long mountId = mMountTable.reverseResolve(path).getMountInfo().getMountId();
    UnderFileSystem client = mMountTable.getUfsClient(mountId).acquireUfsResource().get();
    Executors.newSingleThreadExecutor().submit(() -> {
      Iterator<UfsStatus> ufsStatusIterator = null;
      try {
        ufsStatusIterator = client.listStatusIterable(
            path.getPath(), ListOptions.defaults().setRecursive(recursive), startFrom, 0);
        List<UfsStatus> ufsStatuses = new ArrayList<>();
        while (ufsStatusIterator.hasNext()) {
          if (mCancelled) {
            return;
          }
          UfsStatus ufsStatus = ufsStatusIterator.next();
          ufsStatus.setUfsFullPath(path.join(ufsStatus.getName()));
          ufsStatuses.add(ufsStatus);
          if (ufsStatuses.size() == 3) {
            onBatchFetched.accept(new UfsLoadResult(ufsStatuses, true));
            ufsStatuses = new ArrayList<>();
            CommonUtils.sleepMs(1000);
          }
        }
        onBatchFetched.accept(new UfsLoadResult(ufsStatuses, false));
      }
      catch (Exception e) {
        onFetchFailed.accept(e);
      }
    });
  }
}
