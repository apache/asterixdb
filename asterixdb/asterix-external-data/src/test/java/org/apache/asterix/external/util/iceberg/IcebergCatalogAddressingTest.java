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

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

/**
 * Guards the addressing style used by an Iceberg catalog's FileIO S3 client.
 * <p>
 * A collection declares {@code serviceEndpoint} together with {@code pathStyleAddressing}; the two describe one
 * thing, how to reach the endpoint. {@link IcebergUtils#filterCatalogProperties} carries collection settings into
 * the catalog's FileIO configuration by allow-list, and once carried only {@code serviceEndpoint} and not
 * {@code pathStyleAddressing}. The client was then built with a custom endpoint but
 * {@code forcePathStyle(false)}, which is not "no preference": it moves the bucket out of the path and into the
 * hostname. Against a path-style-only S3-compatible store the read fails, and the user's explicit
 * {@code pathStyleAddressing=true} was discarded without an error.
 * <p>
 * The runtime Iceberg suite cannot catch this. It addresses its mock through {@code http://127.0.0.1:<port>}, and
 * the AWS SDK cannot form {@code bucket.127.0.0.1}, so it falls back to path style whatever {@code forcePathStyle}
 * says; the dropped property is invisible. {@link #ipLiteralEndpointHidesTheAddressingChoice()} pins that masking
 * behaviour so the blind spot stays documented, and the tests here use a hostname endpoint instead. Presigning
 * resolves the request URI offline, so none of this needs DNS, a container, or a network.
 */
public class IcebergCatalogAddressingTest {

    private static final String ENDPOINT_HOSTNAME = "http://iceberg-storage:9090";
    private static final String ENDPOINT_IP_LITERAL = "http://127.0.0.1:9090";
    private static final String BUCKET = "iceberg-container";
    private static final String KEY = "nessie/warehouse/metadata.json";
    private static final String REGION = "us-west-2";

    /**
     * Builds the configuration a {@code CREATE EXTERNAL COLLECTION ... USING S3} statement produces for an Iceberg
     * collection that references a catalog.
     */
    private static Map<String, String> collectionConfiguration() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE,
                ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
        configuration.put(AwsConstants.SERVICE_END_POINT_FIELD_NAME, ENDPOINT_HOSTNAME);
        configuration.put(S3Constants.PATH_STYLE_ADDRESSING_FIELD_NAME, "true");
        configuration.put(AwsConstants.REGION_FIELD_NAME, REGION);
        configuration.put(AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "dummyAccessKey");
        configuration.put(AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "dummySecretKey");
        configuration.put(IcebergConstants.ICEBERG_CATALOG_NAME, "myNessieCatalog");
        return configuration;
    }

    /**
     * Replays what happens between the DDL and the catalog's FileIO client: the collection configuration is reduced
     * to catalog properties, and {@code EnsureCloseAWSClientFactory.s3()} then reduces those back to the collection
     * properties it hands to {@code S3Utils.buildClient}.
     */
    private static Map<String, String> propertiesReachingTheS3Client() {
        return IcebergUtils.filterCollectionProperties(IcebergUtils.filterCatalogProperties(collectionConfiguration()));
    }

    /**
     * The regression. {@code pathStyleAddressing} must survive into the catalog's FileIO configuration; when it did
     * not, it defaulted to false and the client addressed the endpoint virtual-hosted style.
     */
    @Test
    public void pathStyleAddressingReachesTheCatalogFileIoClient() throws Exception {
        Map<String, String> properties = propertiesReachingTheS3Client();
        Assert.assertEquals("pathStyleAddressing must survive into the catalog's FileIO configuration", "true",
                properties.get(S3Constants.PATH_STYLE_ADDRESSING_FIELD_NAME));
        // this is the exact value S3Utils.buildClient passes to S3ClientBuilder.forcePathStyle
        Assert.assertTrue("the catalog's FileIO client must be built with forcePathStyle(true)",
                AwsUtils.getPathStyleAddressing(properties));
    }

    /**
     * Control: the endpoint has always been carried. Were it to stop being carried the addressing assertions above
     * would still pass while pointing at real AWS, so pin it explicitly.
     */
    @Test
    public void serviceEndpointReachesTheCatalogFileIoClient() {
        Assert.assertEquals(ENDPOINT_HOSTNAME,
                propertiesReachingTheS3Client().get(AwsConstants.SERVICE_END_POINT_FIELD_NAME));
    }

    /**
     * The consequence the propagation protects against, stated in terms of the request that goes on the wire: with a
     * hostname endpoint the addressing choice decides whether the bucket is a path segment or part of the host.
     */
    @Test
    public void hostnameEndpointKeepsBucketInPathOnlyUnderPathStyle() {
        URI pathStyle = resolveRequestUri(ENDPOINT_HOSTNAME, true);
        Assert.assertEquals("iceberg-storage", pathStyle.getHost());
        Assert.assertEquals("/" + BUCKET + "/" + KEY, pathStyle.getPath());

        URI virtualHosted = resolveRequestUri(ENDPOINT_HOSTNAME, false);
        Assert.assertEquals("dropping pathStyleAddressing moves the bucket into the hostname",
                BUCKET + ".iceberg-storage", virtualHosted.getHost());
        Assert.assertEquals("/" + KEY, virtualHosted.getPath());
    }

    /**
     * Why the runtime Iceberg suite cannot catch the regression: against an IP literal the SDK uses path style
     * regardless, so both settings look identical and a dropped {@code pathStyleAddressing} goes unnoticed. If this
     * ever stops holding, the runtime suite gains the coverage and this note should be revisited.
     */
    @Test
    public void ipLiteralEndpointHidesTheAddressingChoice() {
        for (boolean forcePathStyle : new boolean[] { true, false }) {
            URI resolved = resolveRequestUri(ENDPOINT_IP_LITERAL, forcePathStyle);
            Assert.assertEquals("an IP-literal endpoint cannot carry the bucket in its host", "127.0.0.1",
                    resolved.getHost());
            Assert.assertEquals("/" + BUCKET + "/" + KEY, resolved.getPath());
        }
    }

    /**
     * Resolves the URI the SDK would put on the wire, without contacting anything: presigning runs the same endpoint
     * resolution as a real request but performs no DNS lookup and no I/O.
     */
    private static URI resolveRequestUri(String endpoint, boolean forcePathStyle) {
        try (S3Presigner presigner = S3Presigner.builder().region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create("dummyAccessKey", "dummySecretKey")))
                .endpointOverride(URI.create(endpoint))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(forcePathStyle).build())
                .build()) {
            return URI
                    .create(presigner
                            .presignGetObject(
                                    GetObjectPresignRequest.builder().signatureDuration(Duration.ofMinutes(5))
                                            .getObjectRequest(
                                                    GetObjectRequest.builder().bucket(BUCKET).key(KEY).build())
                                            .build())
                            .url().toString());
        }
    }
}
