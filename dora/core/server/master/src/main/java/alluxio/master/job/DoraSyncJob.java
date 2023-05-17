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

package alluxio.master.job;

import static java.util.stream.Collectors.toList;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.SyncJobPOptions;
import alluxio.grpc.SyncRequest;
import alluxio.grpc.SyncResponse;
import alluxio.job.JobDescription;
import alluxio.master.file.meta.MountTable;
import alluxio.master.scheduler.Scheduler;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.underfs.UfsStatus;
import alluxio.wire.WorkerInfo;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Load job that loads a file or a directory into Alluxio.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class DoraSyncJob extends AbstractJob<DoraSyncJob.DoraSyncTask> {
  private static final boolean DEBUG_SKIP_WORKER_RPC = false;

  NaiveSyncHandler mNaiveSyncHandler;
  WorkerLocationPolicy mWorkerLocationPolicy = new WorkerLocationPolicy(2000);
  // Worker id -> queue
  ConcurrentHashMap<Long, SyncingWorkerInfo> mSyncingWorkerInfo =
      new ConcurrentHashMap<>();
  volatile boolean mFetchFinished = false;

  MountTable mMountTable;
  private AlluxioURI mSyncRootUfsPath;
  private AlluxioURI mSyncRootAlluxioPath;

  private SyncJobPOptions mSyncJobPOptions;

  /**
   * *
   * @param syncRootAlluxioPath
   * @param syncJobPOptions
   * @param mountTable
   * @param naiveSyncHandler
   * @param scheduler
   * @throws InvalidPathException
   */
  public DoraSyncJob(
      AlluxioURI syncRootAlluxioPath,
      SyncJobPOptions syncJobPOptions,
      MountTable mountTable,
      NaiveSyncHandler naiveSyncHandler,
      Scheduler scheduler
  ) throws InvalidPathException {
    super(Optional.empty(), UUID.randomUUID().toString());
    super.setMyScheduler(scheduler);

    for (Map.Entry<WorkerInfo, CloseableResource<BlockWorkerClient>> worker :
        mMyScheduler.getActiveWorkers().entrySet()) {
      mSyncingWorkerInfo.put(worker.getKey().getId(), new SyncingWorkerInfo());
    }
    mMountTable = mountTable;
    mSyncRootUfsPath = mMountTable.resolve(syncRootAlluxioPath).getUri();
    mSyncRootAlluxioPath = syncRootAlluxioPath;
    mSyncJobPOptions = syncJobPOptions;

    // Get current active workers & initialize the map
    mNaiveSyncHandler = naiveSyncHandler;
    try {
      mNaiveSyncHandler.fetchUfsStatus(
          mSyncRootUfsPath, syncJobPOptions.getIsRecursive(),
          syncJobPOptions.getStartFrom().equals("") ? null : syncJobPOptions.getStartFrom(),
          this::onBatchFetched, this::onFetchUfsFail);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void onFetchUfsFail(Exception e) {
    System.out.println("Fetch UFS status failed");
    failJob(AlluxioRuntimeException.from(e));
  }

  private void onBatchFetched(UfsLoadResult ufsLoadResult) {
    System.out.println("Batch Fetched " +  ufsLoadResult.getItems().size());
    List<BlockWorkerInfo> candidates = mMyScheduler.getActiveWorkers().keySet().stream()
        .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
        .collect(toList());

    for (UfsStatus status : ufsLoadResult.getItems()) {
      List<BlockWorkerInfo> preferredWorkers =
          mWorkerLocationPolicy.getPreferredWorkers(candidates, status.getName(), 1);
      if (preferredWorkers.size() == 0) {
        mNaiveSyncHandler.cancel();
        failJob(AlluxioRuntimeException.from(new RuntimeException("No worker found")));
        return;
      }
      BlockWorkerInfo blockWorkerInfo = preferredWorkers.get(0);
      WorkerInfo pickedWorkerInfo =
          mMyScheduler.getActiveWorkers().keySet().stream().filter(workerInfo ->
                  workerInfo.getAddress().equals(blockWorkerInfo.getNetAddress()))
              .findFirst().get();
      SyncingWorkerInfo syncingWorkerInfo = mSyncingWorkerInfo.get(pickedWorkerInfo.getId());
      if (syncingWorkerInfo == null) {
        failJob(AlluxioRuntimeException.from(new RuntimeException("Membership change")));
        return;
      }
      System.out.println(
          "UFS status in batch: " + status.getName() + " added to " + pickedWorkerInfo);
      syncingWorkerInfo.getQueue().add(status);
    }
    if (!ufsLoadResult.isTruncated()) {
      mFetchFinished = true;
    }
  }

  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(
        mSyncRootAlluxioPath.toString()).setType("sync").build();
  }

  @Override
  public boolean needVerification() {
    return false;
  }

  @Override
  public void failJob(AlluxioRuntimeException reason) {
    setJobState(JobState.FAILED);
    mNaiveSyncHandler.cancel();
    System.out.println("Job Failed");
    reason.printStackTrace();
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED);
    System.out.println("Job Succeeded");
  }

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    if (mState == JobState.SUCCEEDED) {
      return "Succeeded!";
    }
    if (mState == JobState.FAILED) {
      return "Failed";
    }
    return "Syncing... Last processed key " + getSmallestLastProcessedKey();
  }

  @Override
  public boolean isHealthy() {
    return true;
  }

  @Override
  public boolean isCurrentPassDone() {
    return mSyncingWorkerInfo.entrySet().stream().allMatch(it -> it.getValue().isSyncFinished());
  }

  @Override
  public void initiateVerification() {
    // No op for now
  }

  @Override
  public List<DoraSyncTask> getNextTasks(Collection<WorkerInfo> workers) {
    System.out.println("--- Get next tasks ---");
    List<DoraSyncTask> tasks = new ArrayList<>();
    workers.forEach(it -> {
      // Zero copy?
      List<UfsStatus> ufsStatuses = new ArrayList<>();
      SyncingWorkerInfo syncingWorkerInfo = mSyncingWorkerInfo.get(it.getId());
      if (syncingWorkerInfo == null) {
        System.out.println("Queue for worker " + it + " not found.");
      } else if (!syncingWorkerInfo.isLastTaskInitiated()) {
        BlockingQueue<UfsStatus> queue = syncingWorkerInfo.getQueue();
        if (syncingWorkerInfo.getRunningTask() == null
            && (mFetchFinished || queue.size() > 1)) {
          // TODO(yjsnpi) limit the batch size
          queue.drainTo(ufsStatuses);
          System.out.println("fetched finished " + mFetchFinished);
          boolean isLastTask = queue.isEmpty() && mFetchFinished;
          if (isLastTask) {
            syncingWorkerInfo.setLastTaskInitiated(true);
          }
          DoraSyncTask task = new DoraSyncTask(
              ufsStatuses, syncingWorkerInfo.getLastSyncedItem(), isLastTask);
          task.setMyRunningWorker(it);
          tasks.add(task);
        }
      }
    });
    return tasks;
  }

  @Override
  public void onTaskSubmitFailure(Task<?> task) {
    System.out.println("Task submit failure --> this should not happen");
    failJob(AlluxioRuntimeException.from(new RuntimeException("Task submit failed")));
  }

  private String getSmallestLastProcessedKey() {
    Optional<java.util.Map.Entry<Long, SyncingWorkerInfo>>
        result = mSyncingWorkerInfo.entrySet().stream().min((lhs, rhs) -> {
          UfsStatus itemL = lhs.getValue().mLastSyncedItem;
          UfsStatus itemR = rhs.getValue().mLastSyncedItem;
          return ObjectUtils.compare(itemL == null ? null : itemL.getName(),
              itemR == null ? null : itemR.getName());
        }
    );
    if (!result.isPresent()) {
      return null;
    }
    if (result.get().getValue().getLastSyncedItem() == null) {
      return null;
    }
    return result.get().getValue().getLastSyncedItem().getName();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.SyncJobEntry.Builder jobEntry = alluxio.proto.journal.Job.SyncJobEntry
        .newBuilder()
        .setJobId(mJobId)
        .setSyncPath(mSyncRootUfsPath.toString())
        .setIsRecursive(mSyncJobPOptions.getIsRecursive())
        .setLoadData(mSyncJobPOptions.getLoadData())
        .setState(JobState.toProto(getJobState()));
    String smallestLastProcessedKey = getSmallestLastProcessedKey();
    if (smallestLastProcessedKey != null) {
      jobEntry.setLastProcessedKey(smallestLastProcessedKey);
    }
    Journal.JournalEntry entry = Journal.JournalEntry
        .newBuilder()
        .setSyncJob(jobEntry.build())
        .build();
    System.out.println("Writing journal entry " + entry);
    return entry;
  }

  @Override
  public boolean processResponse(DoraSyncTask task) {
    try {
      task.getResponseFuture().get();
    } catch (Exception e) {
      throw new RuntimeException();
    }
    SyncingWorkerInfo syncingWorkerInfo = mSyncingWorkerInfo.get(task.getMyRunningWorker().getId());
    if (task.mIsLastTask) {
      syncingWorkerInfo.setSyncFinished(syncingWorkerInfo.mSyncFinished);
    }
    if (task.getFilesToLoad().size() > 0) {
      syncingWorkerInfo.setLastSyncedItem(
          task.getFilesToLoad().get(task.getFilesToLoad().size() - 1));
    }
    // TODO(elega) journal less and smarter
    journalJobUpdates();
    if (task.mIsLastTask) {
      syncingWorkerInfo.setSyncFinished(true);
    }
    // Must be the last statement
    syncingWorkerInfo.setRunningTask(null);
    return true;
  }

  @Override
  public void updateJob(Job<?> job) {
    System.out.println("update job" + job);
    // No-op
  }

  private class SyncingWorkerInfo {
    private final BlockingQueue<UfsStatus> mQueue = new BlockingArrayQueue<>();
    @Nullable
    private volatile DoraSyncTask mRunningTask;
    private volatile UfsStatus mLastSyncedItem = null;
    private volatile boolean mSyncFinished = false;
    private volatile boolean mLastTaskInitiated = false;

    SyncingWorkerInfo() {
    }

    public boolean isLastTaskInitiated() {
      return mLastTaskInitiated;
    }

    public void setLastTaskInitiated(boolean lastTaskInitiated) {
      mLastTaskInitiated = lastTaskInitiated;
    }

    public BlockingQueue<UfsStatus> getQueue() {
      return mQueue;
    }

    @Nullable
    public DoraSyncTask getRunningTask() {
      return mRunningTask;
    }

    public void setRunningTask(@Nullable DoraSyncTask runningTask) {
      mRunningTask = runningTask;
    }

    public UfsStatus getLastSyncedItem() {
      return mLastSyncedItem;
    }

    public void setLastSyncedItem(UfsStatus lastSyncedItem) {
      mLastSyncedItem = lastSyncedItem;
    }

    public boolean isSyncFinished() {
      return mSyncFinished;
    }

    public void setSyncFinished(boolean syncFinished) {
      mSyncFinished = syncFinished;
    }
  }

  class DoraSyncTask extends Task<SyncResponse> {
    final UfsStatus mPreviousLastItem;
    final boolean mIsLastTask;
    final List<UfsStatus> mFilesToLoad;

    public DoraSyncTask(List<UfsStatus> filesToLoad, UfsStatus previousLastItem,
                        boolean isLastTask) {
      super(DoraSyncJob.this, DoraSyncJob.this.mTaskIdGenerator.incrementAndGet());
      mFilesToLoad = filesToLoad;
      mPreviousLastItem = previousLastItem;
      mIsLastTask = isLastTask;
    }

    public List<UfsStatus> getFilesToLoad() {
      return mFilesToLoad;
    }

    @Override
    protected ListenableFuture<SyncResponse> run(BlockWorkerClient workerClient) {
      final ListenableFuture<SyncResponse> listenableFuture;
      System.out.println("Executing " + Arrays.toString(mFilesToLoad.stream().map(
          UfsStatus::getName).toArray()) + " on " + getMyRunningWorker());
      if (DEBUG_SKIP_WORKER_RPC) {
        listenableFuture = Futures.immediateFuture(SyncResponse.getDefaultInstance());
      }
      else {
        SyncRequest.Builder builder = SyncRequest.newBuilder();
        builder.addAllUfsStatuses(mFilesToLoad.stream().map(UfsStatus::toProto).collect(toList()));
        builder.setLoadData(mSyncJobPOptions.getLoadData());
        builder.setBasePath(mSyncRootUfsPath.getPath());
        if (mPreviousLastItem != null) {
          builder.setPreviousLast(mPreviousLastItem.toProto());
        }
        listenableFuture = workerClient.sync(builder.build());
      }
      return listenableFuture;
    }
  }
}
