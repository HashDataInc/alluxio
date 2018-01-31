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

package alluxio.underfs.txcos;

import javax.annotation.concurrent.NotThreadSafe;
import alluxio.underfs.MultiRangeObjectInputStream;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream for reading a file from Tencent COS. This input stream returns 0 when calling read with an
 * empty buffer.
 */
@NotThreadSafe
public class TXCOSInputStream extends MultiRangeObjectInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TXCOSInputStream.class);

  /** The oss client for OSS operations. */
  private final COSClient mClient;

  /** Bucket for operations with TXCOS objects. */
  private final String mBucket;

  /** The path of the object to read. */
  private final String mKey;

  /** The size of the object in bytes. */
  private final long mContentLength;

  /**
   * Creates a new instance of {@link TXCOSInputStream}.
   *
   * @param bucket the bucket for operations with TXCOS objects
   * @param key the key of the file
   * @throws IOException if an I/O error occurs
   */
  public TXCOSInputStream(String bucket, String key, COSClient client) throws IOException{
    this(bucket, key, client,0L);
  }

  /**
   * Creates a new instance of {@link TXCOSInputStream}.
   *
   * @param bucketName the bucket name
   * @param key the key of the file
   * @param position the position to begin reading from
   * @throws IOException if an I/O error occurs
   */
  public TXCOSInputStream(String bucketName, String key, COSClient client, long position)
          throws IOException {
    this.mBucket = bucketName;
    this.mKey = key;
    this.mClient = client;
    this.mPos = position;
    ObjectMetadata meta = mClient.getObjectMetadata(mBucket, key);
    mContentLength = meta == null ? 0 : meta.getContentLength();
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    GetObjectRequest req = new GetObjectRequest(mBucket, mKey);
    // Cos returns entire object if we read past the end
    req.setRange(startPos, endPos < mContentLength ? endPos - 1 : mContentLength - 1);
    COSObject ossObject = mClient.getObject(req);
    return new BufferedInputStream(ossObject.getObjectContent());
  }
}
