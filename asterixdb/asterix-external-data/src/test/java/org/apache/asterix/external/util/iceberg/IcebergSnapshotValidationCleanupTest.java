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
package org.apache.asterix.external.util.iceberg;

import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry;
import org.apache.iceberg.catalog.Catalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * {@link IcebergSnapshotUtils#validateSnapshotExists} builds its own catalog to look the snapshot up, and the catalog's
 * AWS clients are held by the static {@link EnsureCloseClientsFactoryRegistry} until they are released, so every exit
 * from it has to release them: a leaked entry is never collected and costs one set of clients per validated DDL.
 * <p>
 * A local server stands in for both Glue and S3. Glue requests are told apart by their {@code X-Amz-Target} header;
 * anything else is an S3 GET for the table's metadata file. The bucket name is not a valid host name, which makes the
 * S3 client address it path-style on the local endpoint.
 */
public class IcebergSnapshotValidationCleanupTest {

    private static final String BUCKET = "test_bucket";
    private static final String METADATA_KEY = "tbl/metadata/v1.metadata.json";
    private static final long SNAPSHOT_ID = 42L;

    private HttpServer server;
    private volatile boolean tableExists = true;
    private final AtomicInteger metadataReads = new AtomicInteger();
    private volatile boolean failClientRelease;
    private Set<String> factoryIdsBefore;

    @Before
    public void startServer() throws Exception {
        factoryIdsBefore = registeredFactoryIds();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void existingSnapshotReleasesClients() throws Exception {
        int registeredBefore = registeredFactoryCount();
        IcebergSnapshotUtils.validateSnapshotExists(properties(SNAPSHOT_ID));
        Assert.assertTrue("the validation must have loaded the table's metadata", metadataReads.get() > 0);
        Assert.assertEquals("a successful validation must release its catalog's clients", registeredBefore,
                registeredFactoryCount());
    }

    @Test
    public void missingSnapshotReleasesClients() throws Exception {
        int registeredBefore = registeredFactoryCount();
        try {
            IcebergSnapshotUtils.validateSnapshotExists(properties(7L));
            Assert.fail("a snapshot the table does not have must be rejected");
        } catch (CompilationException e) {
            Assert.assertEquals(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND.intValue(), e.getErrorCode());
        }
        Assert.assertEquals("a rejected snapshot must still release the catalog's clients", registeredBefore,
                registeredFactoryCount());
    }

    @Test
    public void missingTableReleasesClients() throws Exception {
        tableExists = false;
        int registeredBefore = registeredFactoryCount();
        try {
            IcebergSnapshotUtils.validateSnapshotExists(properties(SNAPSHOT_ID));
            Assert.fail("a table the catalog does not have must be rejected");
        } catch (Exception expected) {
            // the failure itself is not what is under test here
        }
        Assert.assertEquals("a failed table load must still release the catalog's clients", registeredBefore,
                registeredFactoryCount());
    }

    @Test
    public void missingSnapshotIsReportedWhenReleasingClientsFails() throws Exception {
        failClientRelease = true;
        int registeredBefore = registeredFactoryCount();
        try {
            IcebergSnapshotUtils.validateSnapshotExists(properties(7L));
            Assert.fail("a snapshot the table does not have must be rejected");
        } catch (CompilationException e) {
            Assert.assertEquals("a failed release must not replace the validation's own failure",
                    ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND.intValue(), e.getErrorCode());
            Assert.assertEquals("the failed release must be kept, suppressed", 1, e.getSuppressed().length);
        }
        Assert.assertEquals("the catalog's clients must still be released", registeredBefore, registeredFactoryCount());
    }

    @Test
    public void failedClientReleaseAfterValidationIsACompilationError() throws Exception {
        failClientRelease = true;
        try {
            IcebergSnapshotUtils.validateSnapshotExists(properties(SNAPSHOT_ID));
            Assert.fail("a failed release must be reported");
        } catch (CompilationException e) {
            Assert.assertEquals(ErrorCode.EXTERNAL_SOURCE_ERROR.intValue(), e.getErrorCode());
        }
    }

    @Test
    public void catalogAndClientReleaseFailuresAreBothReported() throws Exception {
        String factoryId = "cleanup-test-" + System.nanoTime();
        EnsureCloseClientsFactoryRegistry.register(factoryId, () -> {
            throw new IllegalStateException("release failed");
        });
        Catalog catalog = (Catalog) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { Catalog.class, AutoCloseable.class }, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        throw new IOException("close failed");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY, factoryId);
        try {
            IcebergUtils.closeAndCleanup(catalog, catalogProperties);
            Assert.fail("the failed close must be reported");
        } catch (CompilationException e) {
            Assert.assertTrue(e.getCause() instanceof IOException);
            Assert.assertEquals(1, e.getCause().getSuppressed().length);
            Assert.assertTrue(e.getCause().getSuppressed()[0].getCause() instanceof IllegalStateException);
        }
        Assert.assertFalse("the clients must be released even though the catalog failed to close",
                registeredFactoryIds().contains(factoryId));
    }

    private Map<String, String> properties(long snapshotId) {
        String region = "us-east-1";
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        Map<String, String> properties = new HashMap<>();
        properties.put(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
        properties.put(IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY, "ns");
        properties.put(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY, "tbl");
        properties.put(IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY, Long.toString(snapshotId));
        properties.put(AwsConstants.REGION_FIELD_NAME, region);
        properties.put(AwsConstants.SERVICE_END_POINT_FIELD_NAME, endpoint);
        properties.put(AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "test-access-key");
        properties.put(AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "test-secret-key");
        putCatalogProperty(properties, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, "AWS_GLUE");
        putCatalogProperty(properties, AwsConstants.REGION_FIELD_NAME, region);
        putCatalogProperty(properties, AwsConstants.SERVICE_END_POINT_FIELD_NAME, endpoint);
        putCatalogProperty(properties, AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "test-access-key");
        putCatalogProperty(properties, AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "test-secret-key");
        return properties;
    }

    private void handle(HttpExchange exchange) throws java.io.IOException {
        String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
        if ("AWSGlue.GetDatabase".equals(target)) {
            respond(exchange, 200, "{\"Database\":{\"Name\":\"ns\"}}", null);
        } else if ("AWSGlue.GetTable".equals(target) && tableExists) {
            if (failClientRelease) {
                makeNewClientsFailToRelease();
            }
            respond(exchange, 200,
                    "{\"Table\":{\"Name\":\"tbl\",\"DatabaseName\":\"ns\",\"Parameters\":{"
                            + "\"table_type\":\"ICEBERG\",\"metadata_location\":\"s3://" + BUCKET + "/" + METADATA_KEY
                            + "\"}}}",
                    null);
        } else if (target != null) {
            respond(exchange, 400, "{\"__type\":\"EntityNotFoundException\",\"Message\":\"not found\"}",
                    "EntityNotFoundException");
        } else if (exchange.getRequestURI().getPath().equals("/" + BUCKET + "/" + METADATA_KEY)) {
            metadataReads.incrementAndGet();
            respond(exchange, 200, metadataJson(), null);
        } else {
            respond(exchange, 404, "", null);
        }
    }

    private static void respond(HttpExchange exchange, int status, String body, String errorType)
            throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.1");
        if (errorType != null) {
            exchange.getResponseHeaders().add("x-amzn-ErrorType", errorType);
        }
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }

    private static String metadataJson() {
        String location = "s3://" + BUCKET + "/tbl";
        return "{\"format-version\":2,\"table-uuid\":\"9c12d441-03fe-4693-9a96-a0705ddf69c1\",\"location\":\""
                + location + "\",\"last-sequence-number\":1,\"last-updated-ms\":1700000000000,\"last-column-id\":1,"
                + "\"current-schema-id\":0,\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,"
                + "\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}],\"default-spec-id\":0,"
                + "\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],\"last-partition-id\":999,"
                + "\"default-sort-order-id\":0,\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},"
                + "\"current-snapshot-id\":" + SNAPSHOT_ID + ",\"snapshots\":[{\"sequence-number\":1,"
                + "\"snapshot-id\":" + SNAPSHOT_ID + ",\"timestamp-ms\":1700000000000,\"summary\":{\"operation\":"
                + "\"append\"},\"manifest-list\":\"" + location + "/metadata/snap-42.avro\",\"schema-id\":0}],"
                + "\"snapshot-log\":[{\"timestamp-ms\":1700000000000,\"snapshot-id\":" + SNAPSHOT_ID + "}],"
                + "\"metadata-log\":[]}";
    }

    private static void putCatalogProperty(Map<String, String> properties, String key, String value) {
        properties.put(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + key, value);
    }

    // The catalog under test registers its clients under an id of its own making, so it is found as the one id the
    // registry did not hold before the test began.
    private void makeNewClientsFailToRelease() {
        try {
            for (String id : registeredFactoryIds()) {
                if (!factoryIdsBefore.contains(id)) {
                    EnsureCloseClientsFactoryRegistry.register(id, () -> {
                        throw new IllegalStateException("release failed");
                    });
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Set<String> registeredFactoryIds() throws Exception {
        Field registry = EnsureCloseClientsFactoryRegistry.class.getDeclaredField("REGISTRY");
        registry.setAccessible(true);
        Set<String> ids = new HashSet<>();
        for (Object id : ((Map<?, ?>) registry.get(null)).keySet()) {
            ids.add((String) id);
        }
        return ids;
    }

    private static int registeredFactoryCount() throws Exception {
        Field registry = EnsureCloseClientsFactoryRegistry.class.getDeclaredField("REGISTRY");
        registry.setAccessible(true);
        return ((Map<?, ?>) registry.get(null)).size();
    }
}
