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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.SyncJobPOptions;
import alluxio.job.JobDescription;
import alluxio.job.SyncJobRequest;
import alluxio.util.CommonUtils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads metadata about a path in the UFS to Alluxio. No data will be transferred.
 * This command is a client-side optimization without storing all returned `ls`
 * results, preventing OOM for massive amount of small files.
 */
@ThreadSafe
@PublicApi
public class LoadMetadataCommand extends AbstractFileSystemCommand {
  private static final String JOB_TYPE = "sync";

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("load metadata subdirectories recursively")
          .build();

  private static final Option FORCE_OPTION =
      Option.builder("F")
          .required(false)
          .hasArg(false)
          .desc("update the metadata of the existing sub file forcibly")
          .build();

  private static final Option LOAD_DATA_OPTION =
      Option.builder("l")
          .required(false)
          .hasArg(false)
          .desc("load data alongside the sync")
          .build();

  /**
   * Constructs a new instance to load metadata for the given Alluxio path from UFS.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LoadMetadataCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "loadMetadata";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(RECURSIVE_OPTION)
        .addOption(LOAD_DATA_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    loadMetadata(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()),
        cl.hasOption(LOAD_DATA_OPTION.getOpt()));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  private void loadMetadata(AlluxioURI path, boolean recursive, boolean loadData)
      throws IOException {
    try {
      SyncJobPOptions options =
          SyncJobPOptions.newBuilder().setIsRecursive(recursive).setLoadData(loadData).build();
      SyncJobRequest job = new SyncJobRequest(path.toString(), options);
      mFileSystem.submitJob(job);
      while (true) {
        int v = getProgress(path, JobProgressReportFormat.TEXT, true);
        if (v < 0) {
          break;
        }
        CommonUtils.sleepMs(1000);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int getProgress(
      AlluxioURI path, JobProgressReportFormat format, boolean verbose) {
    try {
      String progress = mFileSystem.getJobProgress(JobDescription
          .newBuilder()
          .setPath(path.getPath())
          .setType(JOB_TYPE)
          .build(), format, verbose);
      System.out.println(progress);
      if (progress.contains("Succeeded") || progress.contains("Failed")) {
        return -3;
      }
      return 0;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        System.out.println("Load for path '" + path + "' cannot be found.");
        return -2;
      }
      System.out.println("Failed to get progress for load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  @Override
  public String getUsage() {
    return "loadMetadata [-R] [-l] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads metadata for the given Alluxio path from the under file system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
