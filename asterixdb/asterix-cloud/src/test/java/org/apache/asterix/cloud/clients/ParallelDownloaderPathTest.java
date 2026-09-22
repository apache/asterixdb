/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.cloud.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.junit.Test;

/**
 * Covers {@link AbstractParallelDownloader#toLocalFile}, the object key to local file mapping shared by every
 * backend's downloader.
 * <p>
 * An object key is always the configured prefix followed by the file's device-relative path, because that is how
 * {@code AbstractCloudIOManager#open} names every object it writes. Stripping exactly that prefix is therefore the
 * whole mapping. The previous implementation instead searched the key for the literal {@code "storage"}, which is
 * correct only while {@code storage} is the first segment under the prefix -- true for data, false for a UDF
 * library at {@code applications/library/storage/...}, which is what broke library deployment on Azure Blob
 * Storage.
 */
public class ParallelDownloaderPathTest {

    private static final IODeviceHandle DEVICE_0 = new IODeviceHandle(new File("/mnt/d0"), "."); // NOSONAR
    private static final IODeviceHandle DEVICE_1 = new IODeviceHandle(new File("/mnt/d1"), "."); // NOSONAR

    private static final String DATA = "storage/partition_3/Default/dv/Idx/0/Idx";
    private static final String LIBRARY = "applications/library/storage/Default/dv/mylib/desc.json";

    @Test
    public void dataPathWithoutPrefixIsUnchanged() {
        assertRelativePath("", DATA, DATA);
    }

    @Test
    public void dataPathHasOnlyThePrefixStripped() {
        assertRelativePath("udf1186/", "udf1186/" + DATA, DATA);
    }

    /**
     * The regression: the old mapping returned {@code storage/Default/dv/mylib/desc.json} here, dropping
     * {@code applications/library/}, because it cut the key at the first occurrence of {@code "storage"}.
     */
    @Test
    public void libraryPathKeepsApplicationsLibrary() {
        assertRelativePath("", LIBRARY, LIBRARY);
    }

    @Test
    public void prefixedLibraryPathKeepsApplicationsLibrary() {
        assertRelativePath("udf1186/", "udf1186/" + LIBRARY, LIBRARY);
    }

    /**
     * A prefix that itself contains {@code storage} defeated the old mapping even for plain data keys: it cut at
     * the occurrence inside the prefix, yielding {@code storage/mystorage/storage/partition_3/...}.
     */
    @Test
    public void prefixContainingStorageIsStrippedWhole() {
        assertRelativePath("mystorage/", "mystorage/" + DATA, DATA);
    }

    @Test
    public void keyNotUnderThePrefixIsLeftAlone() {
        assertRelativePath("udf1186/", "somewhere/else/file", "somewhere/else/file");
    }

    /**
     * The device is taken from the directory the download was requested for, never derived from the key -- an
     * object key does not record which local IO device a node keeps the file on.
     */
    @Test
    public void deviceComesFromTheRequestedDirectory() {
        TestDownloader downloader = new TestDownloader("udf1186/");
        FileReference onDevice1 = new FileReference(DEVICE_1, "applications/library/storage/Default/dv/mylib");
        FileReference result = downloader.map(onDevice1, "udf1186/" + LIBRARY);
        assertSame(DEVICE_1, result.getDeviceHandle());
    }

    private static void assertRelativePath(String prefix, String objectKey, String expectedRelativePath) {
        TestDownloader downloader = new TestDownloader(prefix);
        FileReference directory = new FileReference(DEVICE_0, "unused/for/this/assertion");
        FileReference result = downloader.map(directory, objectKey);
        assertEquals(expectedRelativePath, result.getRelativePath());
        assertSame(DEVICE_0, result.getDeviceHandle());
    }

    /** Minimal concrete downloader: the mapping under test needs nothing but a prefix. */
    private static final class TestDownloader extends AbstractParallelDownloader {

        private final String prefix;

        private TestDownloader(String prefix) {
            this.prefix = prefix;
        }

        FileReference map(FileReference directory, String objectKey) {
            return toLocalFile(directory, objectKey);
        }

        @Override
        protected String getPrefix() {
            return prefix;
        }

        @Override
        protected Set<FileReference> downloadDirectories(Collection<FileReference> toDownload) {
            return Collections.emptySet();
        }

        @Override
        public void downloadFiles(Collection<FileReference> toDownload) {
            // not exercised
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
