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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link TXCOSUnderFileSystem}. It will ensure TXCOS credentials are
 * present before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class TXCOSUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link TXCOSUnderFileSystemFactory}.
   */
  public TXCOSUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkTXCOSCredentials(conf)) {
      try {
        return TXCOSUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "TXCOS Credentials not available, cannot create TXCOS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_TXCOS);
  }

  /**
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean checkTXCOSCredentials(UnderFileSystemConfiguration conf) {
    return conf.containsKey(PropertyKey.TXCOS_ACCESS_KEY)
        && conf.containsKey(PropertyKey.TXCOS_SECRET_KEY)
        && conf.containsKey(PropertyKey.TXCOS_APPID);
  }
}
