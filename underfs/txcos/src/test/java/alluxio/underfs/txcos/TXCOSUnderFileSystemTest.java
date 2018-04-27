/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.txcos;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import alluxio.underfs.txcos.TXCOSUnderFileSystem;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.*;
import com.qcloud.cos.model.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Unit tests for the {@link TXCOSUnderFileSystem}.
 */
public class TXCOSUnderFileSystemTest {

  private TXCOSUnderFileSystem mTXCOSUnderFileSystem;
  private COSClient mClient;
  private String mBucket;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final short BUCKET_MODE = 0;
  private static final String ACCOUNT_OWNER = "account owner";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, CosServiceException {
    mClient = Mockito.mock(COSClient.class);
    mBucket = Mockito.mock(String.class);

    mTXCOSUnderFileSystem = new TXCOSUnderFileSystem(new AlluxioURI(""), mClient,
        BUCKET_NAME, ACCOUNT_OWNER, BUCKET_MODE,
        UnderFileSystemConfiguration.defaults());
  }

  /**
   * Test case for {@link TXCOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws Exception {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(CosServiceException.class);


    boolean result = mTXCOSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link TXCOSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws Exception {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(CosServiceException.class);

    boolean result =
        mTXCOSUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link TXCOSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws Exception {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(CosServiceException.class);


    boolean result = mTXCOSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}
