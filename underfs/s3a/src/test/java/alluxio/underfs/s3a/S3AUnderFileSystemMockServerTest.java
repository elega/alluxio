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

package alluxio.underfs.s3a;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.file.options.DescendantType;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Unit tests for the {@link S3AUnderFileSystem} using a s3 mock server.
 */
public class S3AUnderFileSystemMockServerTest {
  private static final InstancedConfiguration CONF = Configuration.copyGlobal();

  private static String TEST_BUCKET = "test-bucket";
  private static final String TEST_FILE = "test_file";
  private static final AlluxioURI TEST_FILE_URI = new AlluxioURI("s3://test-bucket/test_file");
  private static final String TEST_CONTENT = "test_content";

  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3 mClient;
  private S3AsyncClient mAsyncClient;

  @Rule
  public S3ProxyRule s3Proxy = S3ProxyRule.builder()
      // This is a must to close the behavior gap between native s3 and s3 proxy
      .withBlobStoreProvider("transient")
      .withPort(8001)
      .withCredentials("_", "_")
      .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws AmazonClientException {
    AwsClientBuilder.EndpointConfiguration
        endpoint = new AwsClientBuilder.EndpointConfiguration(
        "http://localhost:8001", "us-west-2");
    mClient = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(s3Proxy.getAccessKey(), s3Proxy.getSecretKey())))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(s3Proxy.getUri().toString(),
                Regions.US_WEST_2.getName()))
        .build();
    mAsyncClient = S3AsyncClient.builder().credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create(s3Proxy.getAccessKey(), s3Proxy.getSecretKey())))
        .endpointOverride(s3Proxy.getUri()).build();
    mClient.createBucket(TEST_BUCKET);

    // Uncomment the followings to test on the s3 bucket
    /*
    mClient = AmazonS3ClientBuilder
        .standard()
        .withRegion(Region.US_WEST_1.toString()).build();
    mAsyncClient = S3AsyncClient.builder().region(Region.US_WEST_1).build();
    TEST_BUCKET = "tyler-alluxio-test-bucket";
     */

    mS3UnderFileSystem =
        new S3AUnderFileSystem(new AlluxioURI("s3://" + TEST_BUCKET), mClient,
            mAsyncClient, TEST_BUCKET,
            Executors.newSingleThreadExecutor(), new TransferManager(),
            UnderFileSystemConfiguration.defaults(CONF), false);
  }

  @After
  public void after() {
    mClient = null;
  }

  @Test
  public void read() throws IOException {
    mClient.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);

    InputStream is =
        mS3UnderFileSystem.open(TEST_FILE_URI.getPath());
    assertEquals(TEST_CONTENT, IOUtils.toString(is, StandardCharsets.UTF_8));
  }

  @Test
  public void nestedDirectory() throws Throwable {
    mClient.putObject(TEST_BUCKET, "d1/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d1/f2", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d1/d2/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d2/d1/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "d3/", "");
    mClient.putObject(TEST_BUCKET, "d4/", "");
    mClient.putObject(TEST_BUCKET, "d4/f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "f1", TEST_CONTENT);
    mClient.putObject(TEST_BUCKET, "f2", TEST_CONTENT);

    /*
      Objects:
       d1/
       d1/d1/
       d1/d1/f1
       d1/d1/f2
       d1/d2/
       d1/d2/f1
       d2/
       d2/d1/
       d2/d1/f1
       d3/
       d4/
       d4/f1
       f1
       f2
     */

    UfsStatus[] ufsStatuses = mS3UnderFileSystem.listStatus(
        "/", ListOptions.defaults().setRecursive(true));
    assertNotNull(ufsStatuses);
    assertEquals(14, ufsStatuses.length);

    // I think this is incorrect....
    // why the items count is 9 instead of 14?
    // Directories like d1/d1 and d1/d2 are not returned as objects
    UfsLoadResult result =  performListingAsyncAndGetResult("/", DescendantType.ALL);
    Assert.assertEquals(9, result.getItemsCount());

    result =  performListingAsyncAndGetResult("/", DescendantType.ONE);
    Assert.assertEquals(6, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d1");
    assertEquals(0, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d1/");
    assertEquals(0, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d3");
    assertEquals(0, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d3/");
    assertEquals(1, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d4");
    assertEquals(0, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("d4/");
    assertEquals(1, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("f1");
    assertEquals(1, result.getItemsCount());

    result =  performGetStatusAsyncAndGetResult("f1/");
    assertEquals(0, result.getItemsCount());
  }

  @Test
  public void iterator() throws IOException {
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        for (int k = 0; k < 5; ++k) {
          mClient.putObject(TEST_BUCKET, String.format("%d/%d/%d", i, j, k), TEST_CONTENT);
        }
      }
    }

    Iterator<UfsStatus> ufsStatusesIterator = mS3UnderFileSystem.listStatusIterable(
        "/", ListOptions.defaults().setRecursive(true), null, 5);
    UfsStatus[] statusesFromListing =
        mS3UnderFileSystem.listStatus("/", ListOptions.defaults().setRecursive(true));
    assertNotNull(statusesFromListing);
    assertNotNull(ufsStatusesIterator);
    UfsStatus[] statusesFromIterator =
        Iterators.toArray(ufsStatusesIterator, UfsStatus.class);
    Arrays.sort(statusesFromListing, Comparator.comparing(UfsStatus::getName));
    assertArrayEquals(statusesFromIterator, statusesFromListing);
  }


  public UfsLoadResult performListingAsyncAndGetResult(String path, DescendantType descendantType)
      throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    AtomicReference<UfsLoadResult> result = new AtomicReference<>();
    mS3UnderFileSystem.performListingAsync(path, null, null, descendantType,
        (r)->{
          result.set(r);
          latch.countDown();
        }, (t)->{
          throwable.set(t);
          latch.countDown();
        });
    latch.await();
    if (throwable.get() != null) {
      throw throwable.get();
    }
    return result.get();
  }

  public UfsLoadResult performGetStatusAsyncAndGetResult(String path)
      throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    AtomicReference<UfsLoadResult> result = new AtomicReference<>();
    mS3UnderFileSystem.performGetStatusAsync(path,
        (r)->{
          result.set(r);
          latch.countDown();
        }, (t)->{
          throwable.set(t);
          latch.countDown();
        });
    latch.await();
    if (throwable.get() != null) {
      throw throwable.get();
    }
    return result.get();
  }

}
