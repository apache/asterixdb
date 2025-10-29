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
package org.apache.asterix.external.util.aws.glue;

import static org.apache.asterix.external.util.aws.AwsConstants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SERVICE_END_POINT_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsUtils.buildCredentialsProvider;
import static org.apache.asterix.external.util.aws.AwsUtils.validateAndGetRegion;

import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.CloseableAwsClients;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

public class GlueUtils {
    private GlueUtils() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration properties
     * @return client
     * @throws CompilationException CompilationException
     */
    public static CloseableAwsClients buildClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        CloseableAwsClients awsClients = new CloseableAwsClients();
        String regionId = configuration.get(REGION_FIELD_NAME);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);

        Region region = validateAndGetRegion(regionId);
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(appCtx, configuration, awsClients);

        GlueClientBuilder builder = GlueClient.builder();
        builder.region(region);
        builder.credentialsProvider(credentialsProvider);
        AwsUtils.setEndpoint(builder, serviceEndpoint);

        awsClients.setConsumingClient(builder.build());
        return awsClients;
    }
}
