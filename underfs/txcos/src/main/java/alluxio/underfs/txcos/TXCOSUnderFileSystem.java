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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.*;
import com.qcloud.cos.exception.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class TXCOSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(TXCOSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** TXCOS service. */
  private final COSClient mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The name of the TXCOS bucket owner. */
  private final String mOwner;

  /** The permission mode that the account owner has to the bucket. */
  private final short mBucketMode;

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * Constructs a new instance of {@link TXCOSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @return the created {@link TXCOSUnderFileSystem} instance
   * @throws Exception when a connection to GCS could not be created
   */
  public static TXCOSUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) throws Exception {

    String bucketName = uri.getHost();

    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.TXCOS_APPID),
        "Property " + PropertyKey.TXCOS_APPID + " is required to connect to TXCOS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.TXCOS_ACCESS_KEY),
        "Property " + PropertyKey.TXCOS_ACCESS_KEY + " is required to connect to TXCOS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.TXCOS_SECRET_KEY),
        "Property " + PropertyKey.TXCOS_SECRET_KEY + " is required to connect to TXCOS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.TXCOS_ZONE),
        "Property " + PropertyKey.TXCOS_ZONE + " is required to connect to TXCOS");

    String appId = Configuration.get(PropertyKey.TXCOS_APPID);
    String accessId = Configuration.get(PropertyKey.TXCOS_ACCESS_KEY);
    String accessKey = Configuration.get(PropertyKey.TXCOS_SECRET_KEY);
    String zone = Configuration.get(PropertyKey.TXCOS_ZONE);

    // get TXCOS service
    COSCredentials cred = new BasicCOSCredentials(accessId, accessKey);
    ClientConfig clientConfig = new ClientConfig(new Region(zone));
    COSClient cosclient = new COSClient(cred, clientConfig);

    // get TXCOS owner
    //String bucketNameInternal = new StringBuilder().append(bucketName).append("-").append(appId).toString();
    AccessControlList aclGet = cosclient.getBucketAcl(bucketName);
    List<Grant> grants = aclGet.getGrantsAsList();
    String owner = grants.get(0).getGrantee().getIdentifier();

    // Default to readable and writable by the user.
    short bucketMode = (short) 700;

    return new TXCOSUnderFileSystem(uri, cosclient, bucketName, owner, bucketMode, conf);
  }

  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   * 
   * @param uri the {@link AlluxioURI} used to create this ufs
   * @param mClient
   * @param mBucketName
   * @param mOwner
   * @param mBucketMode
   */
  protected TXCOSUnderFileSystem(AlluxioURI uri, COSClient mClient,
      String mBucketName, String mOwner, short mBucketMode,
      UnderFileSystemConfiguration conf) {
    super(uri, conf);
    this.mClient = mClient;
    this.mBucketName = mBucketName;
    this.mOwner = mOwner;
    this.mBucketMode = mBucketMode;
  }

  @Override
  public String getUnderFSType() {
    return "txcos";
  }

  // Setting TXCOS owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String owner, String group) {}

  // Setting TXCOS mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean createEmptyObject(String key) {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      objMeta.setContentMD5(DIR_HASH);
      mClient.putObject(mBucketName, key, new ByteArrayInputStream(new byte[0]), objMeta);
      return true;
    } catch (CosClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new TXCOSOutputStream(mBucketName, key, mClient);
  }

  @Override
  protected boolean copyObject(String src, String dst) throws IOException {
    try {
      LOG.info("Copying {} to {}", src, dst);
      mClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (CosClientException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (CosClientException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getContentLength(), meta.getLastModified().getTime());
    } catch (CosServiceException e) {
      LOG.warn("Failed to get Object {}, return null", key, e);
      return null;
    }
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(mBucketName);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength());
    request.setDelimiter(delimiter);

    ObjectListing result = getObjectListingChunk(request);
    if (result != null) {
      return new TXCOSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (CosServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over TXCOS {@link TXCOSObjectListingChunk}.
   */
  private final class TXCOSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    TXCOSObjectListingChunk(ListObjectsRequest request, ObjectListing result)
        throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("TXCOS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<COSObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (COSObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getSize(), obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[res.size()]);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        ObjectListing nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new TXCOSObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_TXCOS + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new TXCOSInputStream(mBucketName, key, mClient, options.getOffset());
    } catch (CosServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
