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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A collection's {@code useDeleteCache} is the way back to loading delete files per data file should the shared cache
 * misbehave, so it has to take effect on the nodes: the factory decides when the scan is planned and the decision must
 * survive the factory's trip to the nodes.
 */
public class IcebergDeleteCacheSwitchTest {

    @Test
    public void theCacheIsOnUnlessTheCollectionTurnsItOff() {
        Assert.assertTrue("absent", IcebergParquetRecordReaderFactory.isDeleteCacheEnabled(Map.of()));
        Assert.assertTrue(IcebergParquetRecordReaderFactory.isDeleteCacheEnabled(option("true")));
        Assert.assertTrue(IcebergParquetRecordReaderFactory.isDeleteCacheEnabled(option("TRUE")));
        Assert.assertFalse(IcebergParquetRecordReaderFactory.isDeleteCacheEnabled(option("false")));
        Assert.assertFalse(IcebergParquetRecordReaderFactory.isDeleteCacheEnabled(option("False")));
    }

    /** Anything but a boolean is rejected when the collection is created, rather than silently reading as false. */
    @Test
    public void aValueThatIsNotABooleanIsRejected() throws Exception {
        IcebergUtils.validateIcebergTableProperties(collection("false"));
        try {
            IcebergUtils.validateIcebergTableProperties(collection("ture"));
            Assert.fail("a misspelt boolean must be rejected");
        } catch (CompilationException e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains(ExternalDataConstants.IcebergOptions.USE_DELETE_CACHE));
        }
    }

    private static Map<String, String> collection(String useDeleteCache) {
        return Map.of(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY, "tbl",
                IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY, "ns",
                ExternalDataConstants.IcebergOptions.USE_DELETE_CACHE, useDeleteCache);
    }

    private static Map<String, String> option(String value) {
        return Map.of(ExternalDataConstants.IcebergOptions.USE_DELETE_CACHE, value);
    }

    @Test
    public void disabledReadersLoadTheirOwnDeleteFiles() throws Exception {
        IcebergParquetRecordReaderFactory factory = onNode(factory(false));
        Assert.assertNull("a disabled cache must leave each reader to load its own delete files",
                factory.deleteCache());
    }

    @Test
    public void enabledReadersOfAScanShareOneCache() throws Exception {
        IcebergParquetRecordReaderFactory factory = onNode(factory(true));
        int readers = 16;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<IcebergDeleteCache>> caches = new ArrayList<>();
            for (int r = 0; r < readers; r++) {
                caches.add(pool.submit(() -> {
                    start.await();
                    return factory.deleteCache();
                }));
            }
            start.countDown();
            IcebergDeleteCache first = caches.get(0).get();
            Assert.assertNotNull(first);
            for (Future<IcebergDeleteCache> cache : caches) {
                Assert.assertSame("every reader of the scan on a node must share one cache", first, cache.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static IcebergParquetRecordReaderFactory factory(boolean deleteCacheEnabled) throws Exception {
        IcebergParquetRecordReaderFactory factory = new IcebergParquetRecordReaderFactory();
        // configure() keeps the collection's WITH options here, which needs a catalog to reach
        Field field = IcebergParquetRecordReaderFactory.class.getDeclaredField("originalConfiguration");
        field.setAccessible(true);
        field.set(factory, new HashMap<>(option(Boolean.toString(deleteCacheEnabled))));
        return factory;
    }

    // The factory is configured on the CC and serialized to the nodes, where the readers are created
    private static IcebergParquetRecordReaderFactory onNode(IcebergParquetRecordReaderFactory factory)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(factory);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (IcebergParquetRecordReaderFactory) in.readObject();
        }
    }
}
