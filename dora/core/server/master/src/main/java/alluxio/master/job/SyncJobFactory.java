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
import alluxio.job.SyncJobRequest;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;

/**
 * Factory for creating {@link LoadJob}s that get file infos from master.
 */
public class SyncJobFactory implements JobFactory {

  private final DefaultFileSystemMaster mFsMaster;
  private final SyncJobRequest mRequest;

  /**
   * Create factory.
   * @param request load job request
   * @param fsMaster file system master
   */
  public SyncJobFactory(SyncJobRequest request, DefaultFileSystemMaster fsMaster) {
    mFsMaster = fsMaster;
    mRequest = request;
  }

  @Override
  public Job<?> create() {
    try {
      return new DoraSyncJob(
          new AlluxioURI(mRequest.getPath()),
          mRequest.getOptions(),
          mFsMaster.getMountTable(),
          new NaiveSyncHandler(mFsMaster),
          mFsMaster.getScheduler());
    } catch (InvalidPathException e) {
      throw new RuntimeException(e);
    }
  }
}

