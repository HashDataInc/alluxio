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

package alluxio.underfs.ks3;

import javax.annotation.concurrent.NotThreadSafe;
import alluxio.underfs.MultiRangeObjectInputStream;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;


import com.ksyun.ks3.service.Ks3;
import com.ksyun.ks3.service.request.GetObjectRequest;
import com.ksyun.ks3.dto.ObjectMetadata;
import com.ksyun.ks3.dto.GetObjectResult;
import com.ksyun.ks3.dto.Ks3Object;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream for reading a file from KS3. This input stream returns 0 when calling read with an
 * empty buffer.
 */
@NotThreadSafe
public class KS3InputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(KS3InputStream.class);

  /** The oss client for OSS operations. */
  private final Ks3 mClient;

  /** Bucket for operations with KS3 objects. */
  private final String mBucket;

  /** The path of the object to read. */
  private final String mKey;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Creates a new instance of {@link KS3InputStream}.
   *
   * @param bucket the bucket for operations with KS3 objects
   * @param key the key of the file
   * @throws IOException if an I/O error occurs
   */
  public KS3InputStream(String bucket, String key, Ks3 client) throws IOException{
    this(bucket, key, client,0L);
  }

  /**
   * Creates a new instance of {@link KS3InputStream}.
   *
   * @param bucketName the bucket name
   * @param key the key of the file
   * @param position the position to begin reading from
   * @throws IOException if an I/O error occurs
   */
  public KS3InputStream(String bucketName, String key, Ks3 client, long position)
          throws IOException {
    this.mBucket = bucketName;
    this.mKey = key;
    this.mClient = client;
    this.mPos = position;
    GetObjectRequest request = new GetObjectRequest(bucketName,key);
    GetObjectResult result = client.getObject(request);
    Ks3Object object = result.getObject();
    ObjectMetadata meta = object.getObjectMetadata();
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    GetObjectRequest req = new GetObjectRequest(mBucket, mKey);
    GetObjectResult result = mClient.getObject(req);
    // Ks3 returns entire object if we read past the end
    req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    Ks3Object ks3Object = result.getObject();
    return new BufferedInputStream(ks3Object.getObjectContent());
  }
}
