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

import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.sun.net.httpserver.HttpServer;

/**
 * The reader is constructed inside {@code GenericAdapterFactory.createAdapter}, which is synchronized and shared by
 * every partition on a node, so it must not reach the catalog until it is read from. These tests pin that down, and
 * that a table load failing on the read path still releases the catalog's clients — whether it fails inside
 * catalog initialization, which never hands the catalog back, or after it, when only the reader holds it.
 * <p>
 * The catalog is a Glue catalog pointed at a local stand-in for the Glue endpoint that counts requests. It answers
 * {@code GetDatabase} as {@link #databaseExists} says, and reports every table as missing.
 */
public class IcebergFileRecordReaderLifecycleTest {

    private HttpServer glue;
    private final AtomicInteger glueRequests = new AtomicInteger();
    private final AtomicInteger storageRequests = new AtomicInteger();
    private final AtomicInteger storageHeadRequests = new AtomicInteger();
    private volatile boolean databaseExists;

    @Before
    public void startGlue() throws Exception {
        glue = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        glue.createContext("/", exchange -> {
            String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
            (target != null ? glueRequests : storageRequests).incrementAndGet();
            if (target == null && "HEAD".equals(exchange.getRequestMethod())) {
                storageHeadRequests.incrementAndGet();
            }
            boolean found = "AWSGlue.GetDatabase".equals(target) && databaseExists;
            byte[] body = (found ? "{\"Database\":{\"Name\":\"ns\"}}"
                    : "{\"__type\":\"EntityNotFoundException\",\"Message\":\"not found\"}")
                            .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.1");
            if (!found) {
                exchange.getResponseHeaders().add("x-amzn-ErrorType", "EntityNotFoundException");
            }
            exchange.sendResponseHeaders(found ? 200 : 400, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        glue.start();
    }

    @After
    public void stopGlue() {
        glue.stop(0);
    }

    @Test
    public void constructorDoesNotContactCatalog() throws Exception {
        IcebergFileRecordReader reader = newReader();
        try {
            Assert.assertEquals("constructing the reader must not reach the catalog", 0, glueRequests.get());
        } finally {
            reader.close();
        }
    }

    @Test
    public void missingNamespaceReleasesClientsBeforeReaderIsClosed() throws Exception {
        databaseExists = false;
        int registeredBefore = registeredFactoryCount();
        IcebergFileRecordReader reader = newReader();

        assertHasNextFails(reader, ErrorCode.ICEBERG_NAMESPACE_DOES_NOT_EXIST);
        Assert.assertEquals(
                "a catalog that failed to initialize is never handed back, so it must release its own" + " clients",
                registeredBefore, registeredFactoryCount());
        reader.close();
    }

    @Test
    public void missingTableReleasesClientsWhenReaderIsClosed() throws Exception {
        databaseExists = true;
        int registeredBefore = registeredFactoryCount();
        IcebergFileRecordReader reader = newReader();

        assertHasNextFails(reader, ErrorCode.ICEBERG_TABLE_DOES_NOT_EXIST);
        Assert.assertEquals("the catalog is still open after the failed load", registeredBefore + 1,
                registeredFactoryCount());

        reader.close();
        Assert.assertEquals("closing the reader must release the clients of a catalog whose table load failed",
                registeredBefore, registeredFactoryCount());
    }

    @Test
    public void shippedFileIoReadsWithoutCatalogAndCloseReleasesItsClients() throws Exception {
        IcebergFileIoDescriptor descriptor = shippedFileIo();
        int registeredBefore = registeredFactoryCount();
        IcebergFileRecordReader reader = readerOverShippedFileIo(descriptor);

        attemptRead(reader);
        Assert.assertEquals("a reader given the table's FileIO must not contact the catalog", 0, glueRequests.get());
        Assert.assertTrue("the reader must have read through the shipped FileIO", storageRequests.get() > 0);
        Assert.assertEquals("the data file's length is in the manifest, so the reader must not ask storage for it", 0,
                storageHeadRequests.get());
        Assert.assertEquals("the reader's own FileIO holds its clients until the reader is closed",
                registeredBefore + 1, registeredFactoryCount());

        reader.close();
        Assert.assertEquals("closing the reader must release the clients of the FileIO it opened", registeredBefore,
                registeredFactoryCount());
    }

    /**
     * Every partition on a node reads from one deserialized factory, so its readers open FileIOs from the same
     * descriptor. Were those FileIOs to share a registry id, the first reader to close would shut down the clients of
     * the others while they are still reading.
     */
    @Test
    public void readersOverOneDescriptorReleaseOnlyTheirOwnClients() throws Exception {
        IcebergFileIoDescriptor descriptor = shippedFileIo();
        int registeredBefore = registeredFactoryCount();
        IcebergFileRecordReader first = readerOverShippedFileIo(descriptor);
        IcebergFileRecordReader second = readerOverShippedFileIo(descriptor);

        attemptRead(first);
        attemptRead(second);
        Assert.assertEquals("each reader must register its FileIO's clients under an id of its own",
                registeredBefore + 2, registeredFactoryCount());

        first.close();
        Assert.assertEquals("closing one reader must leave the other reader's clients alone", registeredBefore + 1,
                registeredFactoryCount());
        second.close();
        Assert.assertEquals(registeredBefore, registeredFactoryCount());
    }

    private IcebergFileIoDescriptor shippedFileIo() {
        Map<String, String> ioProperties = new HashMap<>();
        ioProperties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
        ioProperties.put(EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY, "compile-time-id");
        putCollectionProperty(ioProperties, AwsConstants.REGION_FIELD_NAME, "us-east-1");
        putCollectionProperty(ioProperties, AwsConstants.SERVICE_END_POINT_FIELD_NAME, endpoint());
        putCollectionProperty(ioProperties, AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "test-access-key");
        putCollectionProperty(ioProperties, AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "test-secret-key");
        return IcebergFileIoDescriptor.capture(CatalogUtil.loadFileIO(S3FileIO.class.getName(), ioProperties, null));
    }

    private IcebergFileRecordReader readerOverShippedFileIo(IcebergFileIoDescriptor descriptor) throws Exception {
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.location()).thenReturn("s3://test_bucket/data/f.parquet");
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(1024L);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.deletes()).thenReturn(List.of());
        Mockito.when(task.residual()).thenReturn(Expressions.alwaysTrue());
        Mockito.when(task.length()).thenReturn(10L);
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        return new IcebergFileRecordReader(List.of(task), schema, schema, descriptor, null, configuration(), null);
    }

    // the stand-in serves no data file, so the read fails; what is under test is what the attempt touched
    private static void attemptRead(IcebergFileRecordReader reader) {
        try {
            reader.hasNext();
            Assert.fail("the stand-in serves no data file, so the read must fail");
        } catch (Exception expected) {
            // expected
        }
    }

    private void assertHasNextFails(IcebergFileRecordReader reader, ErrorCode expected) throws Exception {
        try {
            reader.hasNext();
            Assert.fail("hasNext must fail with " + expected);
        } catch (CompilationException e) {
            Assert.assertEquals(expected.intValue(), e.getErrorCode());
        }
        Assert.assertTrue("the failed load must have reached the catalog", glueRequests.get() > 0);
    }

    private IcebergFileRecordReader newReader() throws Exception {
        // any non-empty task list: a reader with nothing to read never loads the table at all
        List<FileScanTask> tasks = List.of(Mockito.mock(FileScanTask.class));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        return new IcebergFileRecordReader(tasks, schema, null, null, null, configuration(), null);
    }

    private String endpoint() {
        return "http://127.0.0.1:" + glue.getAddress().getPort();
    }

    private Map<String, String> configuration() {
        String region = "us-east-1";
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE,
                ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
        configuration.put(IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY, "ns");
        configuration.put(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY, "users");
        configuration.put(IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY, "1");
        configuration.put(AwsConstants.REGION_FIELD_NAME, region);
        configuration.put(AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "test-access-key");
        configuration.put(AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "test-secret-key");
        putCatalogProperty(configuration, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, "AWS_GLUE");
        putCatalogProperty(configuration, AwsConstants.REGION_FIELD_NAME, region);
        putCatalogProperty(configuration, AwsConstants.SERVICE_END_POINT_FIELD_NAME, endpoint());
        putCatalogProperty(configuration, AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "test-access-key");
        putCatalogProperty(configuration, AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "test-secret-key");
        return configuration;
    }

    private static void putCollectionProperty(Map<String, String> properties, String key, String value) {
        properties.put(IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + key, value);
    }

    private static void putCatalogProperty(Map<String, String> configuration, String key, String value) {
        configuration.put(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + key, value);
    }

    private static int registeredFactoryCount() throws Exception {
        Field registry = EnsureCloseClientsFactoryRegistry.class.getDeclaredField("REGISTRY");
        registry.setAccessible(true);
        return ((Map<?, ?>) registry.get(null)).size();
    }
}
