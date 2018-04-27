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

import alluxio.util.io.PathUtils;
import com.google.common.base.Preconditions;

import com.ksyun.ks3.exception.Ks3ServiceException;
import com.ksyun.ks3.service.Ks3;
import com.ksyun.ks3.dto.ObjectMetadata;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stream for writing a file into KS3. The data will be persisted to a temporary directory on
 * the local disk and copied as a complete file when the {@link #close()} method is called.
 */
public class KS3OutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(KS3OutputStream.class);

  /** The oss client for OSS operations. */
  private final Ks3 mClient;

  /** The Bucket of KS3 for object operations. */
  private final String mBucket;

  /** Key of the file when it is uploaded to OSS. */
  private final String mKey;

  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** The outputstream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** The MD5 hash of the file. */
  private MessageDigest mHash;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Creates a name instance of {@link KS3OutputStream}.
   *
   * @param key the key of the file
   * @param bucket
   * @throws IOException if an I/O error occurs
   */
  public KS3OutputStream(String bucket, String key, Ks3 client) throws IOException {
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "KS3 path must not be null or empty.");
    Preconditions.checkArgument(bucket != null, "KS3 bucket must not be null.");

    this.mKey = key;
    this.mBucket = bucket;
    this.mClient = client;

    mFile = new File(PathUtils.concatPath("/tmp", UUID.randomUUID()));

    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
  }

  /**
   * Writes the given bytes to this output stream. Before close, the bytes are all written to local
   * file.
   *
   * @param b the bytes to write
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  /**
   * Writes the given byte array to this output stream. Before close, the bytes are all written to
   * local file.
   *
   * @param b the byte array
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  /**
   * Writes the given number of bytes from the given byte array starting at the given offset to this
   * output stream. Before close, the bytes are all written to local file.
   *
   * @param b the byte array
   * @param off the start offset in the data
   * @param len the number of bytes to write
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. Before
   * close, the data are flushed to local file.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to KS3. Once the file is uploaded, the temporary file is deleted.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mLocalOutputStream.close();
    try {
      BufferedInputStream in = new BufferedInputStream(
              new FileInputStream(mFile));
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(mFile.length());
      if (mHash != null) {
        byte[] hashBytes = mHash.digest();
        objMeta.setContentMD5(new String(Base64.encodeBase64(hashBytes)));
      }
      mClient.putObject(mBucket, mKey, in, objMeta);
      mFile.delete();
    } catch (Ks3ServiceException e) {
      LOG.error("Failed to upload {}. Temporary file @ {}", mKey, mFile.getPath());
      throw new IOException(e);
    }
  }
}
