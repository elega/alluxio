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

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.SyncJobPOptions;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Factory for creating {@link LoadJob}s from journal entries.
 */
public class JournalSyncJobFactory implements JobFactory {

  private final DefaultFileSystemMaster mFsMaster;

  private final alluxio.proto.journal.Job.SyncJobEntry mJobEntry;

  /**
   * Create factory.
   *
   * @param journalEntry journal entry
   * @param fsMaster     file system master
   */
  public JournalSyncJobFactory(alluxio.proto.journal.Job.SyncJobEntry journalEntry,
                               FileSystemMaster fsMaster) {
    mFsMaster = (DefaultFileSystemMaster) fsMaster;
    mJobEntry = journalEntry;

  }

  @Override
  public Job<?> create() {
    SyncJobPOptions options = SyncJobPOptions.newBuilder()
        .setIsRecursive(mJobEntry.getIsRecursive())
        .setLoadData(mJobEntry.getLoadData())
        .setStartFrom(mJobEntry.getLastProcessedKey())
        .build();
    try {
      return new DoraSyncJob(new AlluxioURI(mJobEntry.getSyncPath()), options,
          mFsMaster.getMountTable(), new NaiveSyncHandler(mFsMaster), mFsMaster.getScheduler());
    } catch (InvalidPathException e) {
      throw new RuntimeException(e);
    }
  }
}

