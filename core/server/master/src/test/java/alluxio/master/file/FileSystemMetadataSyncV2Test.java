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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.ExistsContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.metasync.MetadataSyncContext;
import alluxio.master.file.metasync.SyncFailReason;
import alluxio.master.file.metasync.SyncOperation;
import alluxio.master.file.metasync.SyncResult;
import alluxio.master.file.metasync.TestMetadataSyncer;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.junit.S3ProxyJunitCore;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMetadataSyncV2Test extends FileSystemMasterTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMetadataSyncV2Test.class);
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_BUCKET2 = "test-bucket-2";
  private static final String TEST_FILE = "test_file";
  private static final String TEST_DIRECTORY = "test_directory";
  private static final String TEST_CONTENT = "test_content";
  private static final AlluxioURI UFS_ROOT = new AlluxioURI("s3://test-bucket/");
  private static final AlluxioURI UFS_ROOT2 = new AlluxioURI("s3://test-bucket-2/");
  private static final AlluxioURI MOUNT_POINT = new AlluxioURI("/s3_mount");
  private static final AlluxioURI MOUNT_POINT2 = new AlluxioURI("/s3_mount2");
  private static final AlluxioURI NESTED_MOUNT_POINT = new AlluxioURI("/mnt/nested_s3_mount");
  private static final AlluxioURI NESTED_S3_MOUNT_POINT =
      new AlluxioURI("/s3_mount/nested_s3_mount");
  /**
   * If you see invalid keystore format error when the mock server starts,
   * please install the latest jdk 8.
   */
//  @Rule
//  public final S3MockRule s3MockRule = S3MockRule.builder().silent().withHttpPort(8001).build();

  @Rule
  public S3ProxyRule s3Proxy = S3ProxyRule.builder()
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  private AmazonS3 mS3Client;

  @Override
  public void before() throws Exception {
    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001");
    Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2");
    Configuration.set(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true);
    Configuration.set(PropertyKey.S3A_ACCESS_KEY, s3Proxy.getAccessKey());
    Configuration.set(PropertyKey.S3A_SECRET_KEY, s3Proxy.getSecretKey());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);

    mS3Client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(s3Proxy.getAccessKey(), s3Proxy.getSecretKey())))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(s3Proxy.getUri().toString(),
                Regions.US_WEST_2.getName()))
        .build();
    mS3Client.createBucket(TEST_BUCKET);
    mS3Client.createBucket(TEST_BUCKET2);
    super.before();
  }

  @Test
  public void syncInodeHappyPath()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    // Sync one file from UFS
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), createContext(DescendantType.ONE));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.CREATE, 1L
    ));
    FileInfo info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    assertFalse(info.isFolder());
    assertTrue(info.isCompleted());

    // Sync again, expect no change
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), createContext(DescendantType.ONE));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 1L
    ));

    // Delete the file from UFS, then sync again
    mS3Client.deleteObject(TEST_BUCKET, TEST_FILE);
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), createContext(DescendantType.NONE));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.DELETE, 1L
    ));
    // info = mFileSystemMaster.getFileInfo(MOUNT_POINT.join(TEST_FILE), getNoSync());
    // assertTrue(info.isFolder());
  }


  @Test
  public void syncInodeUfsDown()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException, NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    stopS3Server();
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE), createContext(DescendantType.NONE));
    assertFalse(result.getSuccess());
    assertEquals(SyncFailReason.UFS_IO_FAILURE, result.getFailReason());
    startS3Server();
  }

  @Test
  public void syncDirectoryHappyPath() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "file1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "file3", TEST_CONTENT);

    // To recreate -> content hashes are different
    mFileSystemMaster.createFile(MOUNT_POINT.join("file1"), CreateFileContext.defaults());
    mFileSystemMaster.completeFile(MOUNT_POINT.join("file1"), CompleteFileContext.defaults());

    // To delete -> doesn't exist in UFS
    mFileSystemMaster.createDirectory(MOUNT_POINT.join("directory1"),
        CreateDirectoryContext.defaults());

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, createContext(DescendantType.ONE));
    assertTrue(result.getSuccess());

    assertSyncOperations(result, ImmutableMap.of(
        // file2 & file 3
        SyncOperation.CREATE, 2L,
        // directory1
        SyncOperation.DELETE, 1L,
        // file1
        SyncOperation.RECREATE, 1L,
        // sync root
        SyncOperation.NOOP, 1L
    ));
  }

  @Test
  public void syncDirectoryTestUFSIteration() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    for (int i = 0; i < 100; ++i) {
      mS3Client.putObject(TEST_BUCKET, "file" + i, "");
    }

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal
            (MOUNT_POINT, createContextWithBatchSize(DescendantType.ONE, 10));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.CREATE, 100L,
        SyncOperation.NOOP, 1L
    ));
  }

  @Test
  public void syncDirectoryTestUFSIterationRecursive() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    int filePerDirectory = 5;
    int createdInodeCount = filePerDirectory * filePerDirectory * filePerDirectory +
        filePerDirectory * filePerDirectory + filePerDirectory;
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        for (int k = 0; k < 5; ++k) {
          mS3Client.putObject(TEST_BUCKET, String.format("%d/%d/%d", i, j, k), "");
        }
      }
    }

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(
            MOUNT_POINT, createContextWithBatchSize(DescendantType.ALL, 10));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.CREATE, (long) createdInodeCount,
        SyncOperation.NOOP, 1L
    ));


    result =
        mFileSystemMaster.syncMetadataInternal(
            MOUNT_POINT, createContextWithBatchSize(DescendantType.ALL, 10));
    assertTrue(result.getSuccess());
    // All created node + root were not changed.
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, (long) createdInodeCount + 1
    ));
  }

  @Test
  public void syncNonS3Directory()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    // Create a directory not on local ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/test_directory"),
        CreateDirectoryContext.defaults());
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), createContext(DescendantType.ONE));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 1L,
        SyncOperation.DELETE, 1L,
        SyncOperation.SKIPPED_ON_MOUNT_POINT, 0L
    ));
  }

  @Test
  public void syncNonS3DirectoryShadowingMountPoint()
      throws Exception {
    /*
      / (root) -> local file system (disk)
      /s3_mount -> s3 bucket
      create /s3_mount in the local first system that shadows the mount point and then do a metadata sync
      the sync of the local file system /s3_mount is expected to be skipped
     */

    String localUfsPath = mFileSystemMaster.getMountTable().resolve(MOUNT_POINT).getUri().getPath();
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    assertTrue(new File(localUfsPath).createNewFile());

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), createContext(DescendantType.ONE));
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        // Root (/)
        SyncOperation.NOOP, 1L,
        // Mount point (/s3_mount)
        SyncOperation.SKIPPED_ON_MOUNT_POINT, 1L
    ));
    FileInfo mountPointFileInfo = mFileSystemMaster.getFileInfo(MOUNT_POINT, getNoSync());
    assertTrue(mountPointFileInfo.isMountPoint());
    assertTrue(mountPointFileInfo.isFolder());
  }

  @Ignore
  @Test(expected = InvalidPathException.class)
  public void syncS3DirectoryNestedMount()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mFileSystemMaster.mount(NESTED_S3_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    // In the existing UFS S3 implementation, ufs.exists() always returns true,
    // regardless if an object exists in s3 or not. If the object does not exist,
    // alluxio S3 UFS implementation treats it as a pseudo directory.
    // This essentially makes it impossible to do a nested mount under an s3 mount point.
  }

  @Test
  public void syncNestedMountPointRecursive()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    // mount /s3_mount -> s3://test-bucket
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "foo/baz", TEST_CONTENT);

    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH));
    // mount /mnt/nested_s3_mount -> s3://test-bucket-2
    mFileSystemMaster.mount(NESTED_MOUNT_POINT, UFS_ROOT2, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET2, "foo/bar", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET2, "foo/baz", TEST_CONTENT);

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), createContext(DescendantType.ALL));

    /*
      / (ROOT) -> unchanged (root mount point local fs)
        /s3_mount -> unchanged (mount point s3://test-bucket)
          /foo -> pseudo directory (created)
            /bar -> (created)
            /baz -> (created)
        /mnt -> unchanged
          /nested_s3_mount -> unchanged (mount point s3://test-bucket-2)
            /foo -> pseudo directory (created)
              /bar -> (created)
              /baz -> (created)
     */

    List<FileInfo> inodes = mFileSystemMaster.listStatus(new AlluxioURI("/"), listNoSync(true));
    assertEquals(9, inodes.size());

    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 4L,
        SyncOperation.CREATE, 6L
    ));

    assertEquals(4, (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.NOOP, 0L));
    assertEquals(6,
        (long) result.getSuccessOperationCount().getOrDefault(SyncOperation.CREATE, 0L));
  }

  @Test
  public void testS3Fingerprint() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f3", TEST_CONTENT);

    // Sync to load metadata
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, createContext(DescendantType.ONE));

    assertSyncOperations(result, ImmutableMap.of(
        SyncOperation.NOOP, 1L,
        SyncOperation.CREATE, 3L
    ));

    mS3Client.putObject(TEST_BUCKET, "f1", "");
    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);

    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, createContext(DescendantType.ONE));
    assertSyncOperations(result, ImmutableMap.of(
        // mount point, f1, f3
        SyncOperation.NOOP, 3L,
        // f2
        SyncOperation.RECREATE, 1L
    ));
  }

  @Test
  public void testNonS3Fingerprint() throws Exception {
    // this essentially creates a directory and mode its alluxio directory without
    // syncing the change down to ufs
    mFileSystemMaster.createDirectory(new AlluxioURI("/d"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.THROUGH));
    mFileSystemMaster.delete(new AlluxioURI("/d"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setAlluxioOnly(true)));
    mFileSystemMaster.createDirectory(new AlluxioURI("/d"),
        CreateDirectoryContext.mergeFrom(
                CreateDirectoryPOptions.newBuilder().setMode(new Mode((short) 0777).toProto()))
            .setWriteType(WriteType.MUST_CACHE));

    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(new AlluxioURI("/"), createContext(DescendantType.ONE));

    assertSyncOperations(result, ImmutableMap.of(
        // root
        SyncOperation.NOOP, 1L,
        // d
        SyncOperation.UPDATE, 1L
    ));
  }

  @Test
  public void syncUfsNotFound() throws Exception {
    // Q: how to design the interface for file not found
    SyncResult result = mFileSystemMaster.syncMetadataInternal(
        new AlluxioURI("/non_existing_path"), createContext(DescendantType.ALL));
    assertFalse(result.getSuccess());
    assertEquals(SyncFailReason.FILE_DOES_NOT_EXIST, result.getFailReason());
  }

  // TODO yimin -> this is not correct
  // Two options to deal with unmount-during-sync
  // Option 1: add read lock on the sync path
  // Option 2: cancel the ongoing metadata sync job
  @Test
  public void unmountDuringSync() throws Exception {
    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();

    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    for (int i = 0; i < 100; ++i) {
      mS3Client.putObject(TEST_BUCKET, "file" + i, "");
    }

    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return mFileSystemMaster.syncMetadataInternal(
            MOUNT_POINT, createContextWithBatchSize(DescendantType.ONE, 10));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    syncer.blockUntilNthSyncThenDo(50, () -> mFileSystemMaster.unmount(MOUNT_POINT));
    SyncResult result = syncFuture.get();
    // This is not expected
    assertTrue(mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(true)).size() < 100);
  }

  @Test
  public void concurrentDelete() throws Exception {
    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();

    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    // Create a directory not on s3 ufs
    mFileSystemMaster.createDirectory(MOUNT_POINT.join("/d"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.MUST_CACHE));
    // Create something else into s3
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, createContext(DescendantType.ALL));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    // blocks on the sync of "/d" (the 2nd sync target)
    syncer.blockUntilNthSyncThenDo(2,
        ()-> mFileSystemMaster.delete(MOUNT_POINT.join("/d"), DeleteContext.defaults()));
    SyncResult result = syncFuture.get();
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        // root
        SyncOperation.NOOP, 1L,
        // d
        SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION, 1L,
        // test-file
        SyncOperation.CREATE, 1L
    ));
  }

  @Test
  public void concurrentCreate() throws Exception {
    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();

    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, createContext(DescendantType.ALL));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    // blocks on the sync of "/test_file" (the 2nd sync target)
    syncer.blockUntilNthSyncThenDo(2,
        ()-> mFileSystemMaster.createFile(MOUNT_POINT.join(TEST_FILE),
            CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE)));
    SyncResult result = syncFuture.get();
    assertTrue(result.getSuccess());
    assertSyncOperations(result, ImmutableMap.of(
        // root
        SyncOperation.NOOP, 1L,
        // test-file
        SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION, 1L
    ));
  }

  @Test
  public void concurrentUpdateRoot() throws Exception {
    TestMetadataSyncer syncer = (TestMetadataSyncer) mFileSystemMaster.getMetadataSyncer();

    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    mFileSystemMaster.createFile(MOUNT_POINT.join(TEST_FILE),
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));

    CompletableFuture<SyncResult> syncFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join(TEST_FILE),
            createContext(DescendantType.NONE));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    syncer.blockUntilNthSyncThenDo(1,
        ()-> mFileSystemMaster.delete(MOUNT_POINT.join(TEST_FILE), DeleteContext.defaults()));
    SyncResult result = syncFuture.get();
    assertFalse(result.getSuccess());
    assertEquals(SyncFailReason.CONCURRENT_UPDATE_DURING_SYNC, result.getFailReason());
  }

  private MetadataSyncContext createContext(DescendantType descendantType)
      throws UnavailableException {
    return MetadataSyncContext.Builder.builder(
        mFileSystemMaster.createRpcContext(), descendantType).build();
  }

  private MetadataSyncContext createContextWithBatchSize(
      DescendantType descendantType, int batchSize) throws UnavailableException {
    return MetadataSyncContext.Builder.builder(
        mFileSystemMaster.createRpcContext(), descendantType).setBatchSize(batchSize).build();
  }

  @Test
  public void startAfter() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "f3", TEST_CONTENT);
    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
    // object key.
    MetadataSyncContext context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(), DescendantType.ALL)
        .setStartAfter("f2").build();
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
    assertTrue(result.getSuccess());
    assertEquals(1, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());

    context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(), DescendantType.ALL)
            .setStartAfter("f1").build();
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
    assertTrue(result.getSuccess());
    assertEquals(2, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());

    context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(), DescendantType.ALL)
            .setStartAfter("a").build();
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT, context);
    assertTrue(result.getSuccess());
    assertEquals(3, mFileSystemMaster.listStatus(MOUNT_POINT, listNoSync(false)).size());
  }

  @Test
  public void startAfterAbsolutePath() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "root/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/f3", TEST_CONTENT);
    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
    // object key.
    assertThrows(InvalidPathException.class, () -> {
      MetadataSyncContext context =
          MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
                  DescendantType.ONE)
              .setStartAfter("/random/path").build();
      SyncResult result =
          mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
    });

    MetadataSyncContext context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
                DescendantType.ONE)
            .setStartAfter("/s3_mount/root/f2").build();
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
    assertTrue(result.getSuccess());
    assertEquals(1, mFileSystemMaster.listStatus(MOUNT_POINT.join("root"), listNoSync(false)).size());

    context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(),
                DescendantType.ONE)
            .setStartAfter("/s3_mount/root").build();
    result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
    assertTrue(result.getSuccess());
    assertEquals(3, mFileSystemMaster.listStatus(MOUNT_POINT.join("root"), listNoSync(false)).size());
    // TODO look into WARNING: xattr not supported on root/
  }

  @Test
  public void startAfterRecursive() throws Exception {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());
    mS3Client.putObject(TEST_BUCKET, "root/d1/d1/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/d1/d1/f2", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/d1/d2/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/d1/d2/f3", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/d1/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/d2/f1", TEST_CONTENT);
    mS3Client.putObject(TEST_BUCKET, "root/f1", TEST_CONTENT);
    // The S3 mock server has a bug where 403 is returned if startAfter exceeds the last
    // object key.
    MetadataSyncContext context =
        MetadataSyncContext.Builder.builder(mFileSystemMaster.createRpcContext(), DescendantType.ALL)
            .setStartAfter("d1/d2/f2").build();
    SyncResult result =
        mFileSystemMaster.syncMetadataInternal(MOUNT_POINT.join("root"), context);
    // Files are created recursively so the # of file created in the result is less than
    // the actual # of files created. Checking the alluxio inode tree instead.
    assertTrue(result.getSuccess());
    /*
    (under "/s3_mount/root")
      /d1
        /d2
          /f3
        /f1
      /d2
        /d1
      /f1
     */
    assertEquals(7,
        mFileSystemMaster.listStatus(MOUNT_POINT.join("root"), listNoSync(true)).size());
  }

  private ListStatusContext listSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
        .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.ALWAYS)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build()
        ));
  }


  private ListStatusContext listNoSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
        .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  private GetStatusContext getNoSync() {
    return GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  private ExistsContext existsNoSync() {
    return ExistsContext.mergeFrom(ExistsPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }


  private void stopS3Server() {
    try {
      Field coreField = S3ProxyRule.class.getDeclaredField("core");
      coreField.setAccessible(true);
      S3ProxyJunitCore core = (S3ProxyJunitCore) coreField.get(s3Proxy);
      Field s3ProxyField = S3ProxyJunitCore.class.getDeclaredField("s3Proxy");
      s3ProxyField.setAccessible(true);
      S3Proxy proxy = (S3Proxy) s3ProxyField.get(core);
      proxy.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startS3Server() {
    try {
      Field coreField = S3ProxyRule.class.getDeclaredField("core");
      coreField.setAccessible(true);
      S3ProxyJunitCore core = (S3ProxyJunitCore) coreField.get(s3Proxy);
      Field s3ProxyField = S3ProxyJunitCore.class.getDeclaredField("s3Proxy");
      s3ProxyField.setAccessible(true);
      S3Proxy proxy = (S3Proxy) s3ProxyField.get(core);
      proxy.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void after() throws Exception {
    mS3Client = null;
    super.after();
  }

  private void assertSyncOperations(SyncResult result, Map<SyncOperation, Long> operations) {
    for (SyncOperation operation : SyncOperation.values()) {
      assertEquals(
          "Operation " + operation.toString() + " count not equal",
          result.getSuccessOperationCount().getOrDefault(operation, 0L),
          operations.getOrDefault(operation, 0L)
      );
    }
  }
}
