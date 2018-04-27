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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link KS3UnderFileSystem}. It will ensure KS3 credentials are
 * present before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class KS3UnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link KS3UnderFileSystemFactory}.
   */
  public KS3UnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkKS3Credentials(conf)) {
      try {
        return KS3UnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "KS3 Credentials not available, cannot create KS3 Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_KS3);
  }

  /**
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean checkKS3Credentials(UnderFileSystemConfiguration conf) {
    return conf.containsKey(PropertyKey.KS3_ACCESS_KEY)
        && conf.containsKey(PropertyKey.KS3_SECRET_KEY);
  }
}
