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

package alluxio.underfs.ks3;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import com.ksyun.ks3.dto.*;
import com.ksyun.ks3.service.Ks3Client;
import com.ksyun.ks3.exception.Ks3ServiceException;
import com.ksyun.ks3.service.Ks3ClientConfig;
import com.ksyun.ks3.service.Ks3ClientConfig.PROTOCOL;
import com.ksyun.ks3.service.Ks3;
import com.ksyun.ks3.http.HttpClientConfig;
import com.ksyun.ks3.service.request.GetObjectRequest;
import com.ksyun.ks3.service.request.HeadObjectRequest;
import com.ksyun.ks3.utils.Base64;
import com.ksyun.ks3.exception.Ks3ClientException;
import com.ksyun.ks3.service.request.ListObjectsRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

public class KS3UnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(KS3UnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** KS3 service. */
  private final Ks3 mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The name of the KS3 bucket owner. */
  private final String mOwner;

  /** The permission mode that the account owner has to the bucket. */
  private final short mBucketMode;

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * Constructs a new instance of {@link KS3UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @return the created {@link KS3UnderFileSystem} instance
   * @throws Exception when a connection to GCS could not be created
   */
  public static KS3UnderFileSystem createInstance(AlluxioURI uri,
                                                  UnderFileSystemConfiguration conf) throws Exception {

    String bucketName = uri.getHost();

    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.KS3_ACCESS_KEY),
        "Property " + PropertyKey.KS3_ACCESS_KEY + " is required to connect to KS3");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.KS3_SECRET_KEY),
        "Property " + PropertyKey.KS3_SECRET_KEY + " is required to connect to KS3");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.KS3_ZONE),
        "Property " + PropertyKey.KS3_ZONE + " is required to connect to KS3");

    String accessId = Configuration.get(PropertyKey.KS3_ACCESS_KEY);
    String accessKey = Configuration.get(PropertyKey.KS3_SECRET_KEY);
    String zone = Configuration.get(PropertyKey.KS3_ZONE);

    // get KS3 service
    Ks3ClientConfig config = new Ks3ClientConfig();
    if(zone.equals("ks3-cn-beijing")){
      config.setEndpoint("ks3-cn-beijing.ksyun.com");
    }else if(zone.equals("ks3-cn-shanghai")){
      config.setEndpoint("ks3-cn-shanghai.ksyun.com");
    }else if(zone.equals("ks3-cn-hk-1")){
      config.setEndpoint("ks3-cn-hk-1.ksyun.com");
    }
    config.setDomainMode(false);
    config.setProtocol(PROTOCOL.http);
    config.setPathStyleAccess(false);
    HttpClientConfig hconfig = new HttpClientConfig();
    config.setHttpClientConfig(hconfig);
    Ks3 ks3client = new Ks3Client(accessId,accessKey,config);

    // get KS3 owner
    //String bucketNameInternal = new StringBuilder().append(bucketName).append("-").append(appId).toString();
    AccessControlPolicy aclGet = null;
    aclGet = ks3client.getBucketACL(bucketName);
    String owner = aclGet.getOwner().getId();

    // Default to readable and writable by the user.
    short bucketMode = (short) 700;

    return new KS3UnderFileSystem(uri, ks3client, bucketName, owner, bucketMode, conf);
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
  protected KS3UnderFileSystem(AlluxioURI uri, Ks3 mClient,
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
    return "ks3";
  }

  // Setting KS3 owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String owner, String group) {}

  // Setting KS3 mode via Alluxio is not supported yet. This is a no-op.
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
    } catch (Ks3ClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new KS3OutputStream(mBucketName, key, mClient);
  }

  @Override
  protected boolean copyObject(String src, String dst) throws IOException {
    try {
      LOG.info("Copying {} to {}", src, dst);
      mClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (Ks3ClientException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (Ks3ClientException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      HeadObjectRequest request = new HeadObjectRequest(mBucketName,key);
      HeadObjectResult result = mClient.headObject(request);
      if (result == null) {
        return null;
      }
      ObjectMetadata meta = result.getObjectMetadata();
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getContentLength(), meta.getLastModified().getTime());
    } catch (Ks3ServiceException e) {
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

    ListObjectsRequest request = new ListObjectsRequest(mBucketName);
    request.setMaxKeys(getListingChunkLength());
    request.setPrefix(key);
    request.setDelimiter(delimiter);

    ObjectListing result = null;
    result = getObjectListingChunk(request);
    if (result != null) {
      return new KS3ObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (Ks3ServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over KS3 {@link KS3ObjectListingChunk}.
   */
  private final class KS3ObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    KS3ObjectListingChunk(ListObjectsRequest request, ObjectListing result)
        throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("KS3 listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<Ks3ObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      
      for (Ks3ObjectSummary obj : objects) {
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
          return new KS3ObjectListingChunk(mRequest, nextResult);
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
    return Constants.HEADER_KS3 + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new KS3InputStream(mBucketName, key, mClient, options.getOffset());
    } catch (Ks3ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
