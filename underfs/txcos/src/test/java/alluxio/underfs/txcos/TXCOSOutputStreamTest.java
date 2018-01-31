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

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.*;
import com.qcloud.cos.model.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.DigestOutputStream;

/**
 * Unit tests for the {@link TXCOSOutputStream}.
 */
@RunWith(PowerMockRunner.class)
public class TXCOSOutputStreamTest {
    private COSClient mClient;
    private String mBucket;
    private File mFile;
    private BufferedOutputStream mLocalOutputStream;

    /**
     * The exception expected to be thrown.
     */
    @Rule
    public final ExpectedException mThrown = ExpectedException.none();

    /**
     * Sets the properties and configuration before each test runs.
     *
     * @throws Exception when the {@link HttpURLConnection#getOutputStream()} throws exception
     */
    @Before
    public void before() throws Exception {
        mClient = Mockito.mock(COSClient.class);
        mBucket = Mockito.mock(String.class);
        mFile = Mockito.mock(File.class);
        mLocalOutputStream = Mockito.mock(BufferedOutputStream.class);
    }

    /**
     * Tests to ensure IOException is thrown if {@link FileOutputStream()} throws an IOException.
     *
     * @throws Exception when the IOException is not thrown
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testConstructor() throws Exception {
        PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
        String errorMessage = "protocol doesn't support output";
        PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile)
                .thenThrow(new IOException(errorMessage));
        mThrown.expect(IOException.class);
        mThrown.expectMessage(errorMessage);
        new TXCOSOutputStream(mBucket, "testKey", mClient).close();
    }

    /**
     * Tests to ensure {@link TXCOSOutputStream#write(int)} calls {@link OutputStream#write(int)}.
     *
     * @throws Exception when {@link OutputStream#write(int)} is not called
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testWrite1() throws Exception {
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        stream.write(1);
        stream.close();
        Mockito.verify(mLocalOutputStream).write(1);
    }

    /**
     * Tests to ensure {@link TXCOSOutputStream#write(byte[], int, int)} calls
     * {@link OutputStream#write(byte[], int, int)} .
     *
     * @throws Exception when {@link OutputStream#write(byte[], int, int)} is not called
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testWrite2() throws Exception {
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        byte[] b = new byte[1];
        stream.write(b, 0, 1);
        stream.close();
        Mockito.verify(mLocalOutputStream).write(b, 0, 1);
    }

    /**
     * Tests to ensure {@link TXCOSOutputStream#write(byte[])} calls {@link OutputStream#write(byte[])}.
     *
     * @throws Exception when {@link OutputStream#write(byte[])} is not called
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testWrite3() throws Exception {
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        byte[] b = new byte[1];
        stream.write(b);
        stream.close();
        Mockito.verify(mLocalOutputStream).write(b, 0, 1);
    }

    /**
     * Tests to ensure IOException is thrown
     *
     * @throws Exception when the IOException is thrown
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testCloseError() throws Exception {
        String errorMessage = "Invoke the createEmptyObject method error.";
        BufferedInputStream inputStream = PowerMockito.mock(BufferedInputStream.class);
        PowerMockito.whenNew(BufferedInputStream.class)
                .withArguments(Mockito.any(FileInputStream.class)).thenReturn(inputStream);
        PowerMockito
                .when(mClient.putObject(Mockito.anyString(), Mockito.anyString(),
                        Mockito.any(BufferedInputStream.class), Mockito.any(ObjectMetadata.class)))
                .thenThrow(new CosServiceException(errorMessage));
        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        mThrown.expect(IOException.class);
        mThrown.expectMessage(errorMessage);
        stream.close();
    }

    /**
     * Tests to ensure {@link File#delete()} is called when close the stream.
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testCloseSuccess() throws Exception {
        PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
        FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
        PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile).thenReturn(outputStream);
        FileInputStream inputStream = PowerMockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(mFile).thenReturn(inputStream);

        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        stream.close();
        Mockito.verify(mFile).delete();
    }

    /**
     * Tests to ensure {@link TXCOSOutputStream#flush()} calls {@link OutputStream#flush()}.
     *
     * @throws Exception when flush is not called
     */
    @Test
    @PrepareForTest(TXCOSOutputStream.class)
    public void testFlush() throws Exception {
        PowerMockito.whenNew(BufferedOutputStream.class)
                .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
        TXCOSOutputStream stream = new TXCOSOutputStream(mBucket, "testKey", mClient);
        stream.flush();
        stream.close();
        Mockito.verify(mLocalOutputStream).flush();
    }
}
