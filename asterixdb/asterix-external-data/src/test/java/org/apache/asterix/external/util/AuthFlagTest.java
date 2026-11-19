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
package org.apache.asterix.external.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.AuthenticationType;
import org.apache.asterix.external.util.azure.AzureConstants;
import org.apache.asterix.external.util.azure.AzureUtils;
import org.apache.asterix.external.util.azure.blob.BlobUtils;
import org.apache.asterix.external.util.azure.datalake.DatalakeUtils;
import org.apache.asterix.external.util.google.GCSUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.google.cloud.NoCredentials;

/**
 * The authentication flags {@code instanceProfile}, {@code applicationDefaultCredentials} and {@code managedIdentity}
 * select their method when {@code true} and nothing when {@code false}, in any letter case, and reject any other value.
 */
public class AuthFlagTest {

    private static final String[] ON = { "true", "TRUE", "True" };
    private static final String[] OFF = { "false", "FALSE", "False" };
    private static final String[] INVALID = { "yes", "", "1", "myManagedIdentity" };
    private static final String AZURE_ENDPOINT = "http://127.0.0.1:10000/devstoreaccount1";

    private static Map<String, String> config(String... keyValues) {
        Map<String, String> config = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            config.put(keyValues[i], keyValues[i + 1]);
        }
        return config;
    }

    private static void assertInvalid(String flag, ThrowingRunnable call) {
        CompilationException e = assertThrows(CompilationException.class, call::run);
        assertTrue(e.getMessage(),
                e.getMessage().contains("Invalid value for parameter '" + flag + "', allowed value(s): true, false"));
    }

    private static void assertRejected(String message, ThrowingRunnable call) {
        CompilationException e = assertThrows(CompilationException.class, call::run);
        assertTrue(e.getMessage(), e.getMessage().contains(message));
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    public void awsInstanceProfile() throws Exception {
        for (String on : ON) {
            assertEquals(AuthenticationType.INSTANCE_PROFILE,
                    AwsUtils.getAuthenticationType(config("instanceProfile", on)));
            assertEquals(AuthenticationType.INSTANCE_PROFILE,
                    AwsUtils.getAuthenticationType(config("roleArn", "arn", "instanceProfile", on), true));
        }
        for (String off : OFF) {
            assertEquals(AuthenticationType.ANONYMOUS, AwsUtils.getAuthenticationType(config("instanceProfile", off)));
            assertEquals(AuthenticationType.ACCESS_KEYS, AwsUtils
                    .getAuthenticationType(config("instanceProfile", off, "accessKeyId", "k", "secretAccessKey", "s")));
            // no long-lived credentials left to assume the role with
            assertEquals(AuthenticationType.BAD_AUTHENTICATION,
                    AwsUtils.getAuthenticationType(config("roleArn", "arn", "instanceProfile", off), true));
        }
        for (String invalid : INVALID) {
            assertInvalid("instanceProfile", () -> AwsUtils.getAuthenticationType(config("instanceProfile", invalid)));
        }
    }

    @Test
    public void gcsApplicationDefaultCredentials() {
        for (String on : ON) {
            assertRejected("Parameter 'jsonCredentials' is not allowed if 'applicationDefaultCredentials' is provided",
                    () -> GCSUtils.buildCredentials(null,
                            config("applicationDefaultCredentials", on, "jsonCredentials", "{}"), null));
        }
        for (String off : OFF) {
            assertAnonymous(() -> GCSUtils.buildCredentials(null, config("applicationDefaultCredentials", off), null));
            // the JSON credentials are used; this key is not valid, so reading it fails, but not as a conflict
            CompilationException e = assertThrows(CompilationException.class, () -> GCSUtils.buildCredentials(null,
                    config("applicationDefaultCredentials", off, "jsonCredentials", "{}"), null));
            assertFalse(e.getMessage(), e.getMessage().contains("applicationDefaultCredentials"));
            assertRejected("No valid authentication parameters were provided to impersonate service account",
                    () -> GCSUtils.buildCredentials(null,
                            config("applicationDefaultCredentials", off, "impersonateServiceAccount", "sa"), null));
        }
        for (String invalid : INVALID) {
            assertInvalid("applicationDefaultCredentials",
                    () -> GCSUtils.buildCredentials(null, config("applicationDefaultCredentials", invalid), null));
        }
    }

    private static void assertAnonymous(CredentialsCall call) {
        try {
            assertSame(NoCredentials.getInstance(), call.get());
        } catch (CompilationException e) {
            throw new AssertionError(e);
        }
    }

    private interface CredentialsCall {
        Object get() throws CompilationException;
    }

    @Test
    public void azureManagedIdentity() throws Exception {
        ThrowingBuilder[] builders = { c -> BlobUtils.buildClient(null, c), c -> DatalakeUtils.buildClient(null, c) };
        for (ThrowingBuilder builder : builders) {
            for (String on : ON) {
                assertRejected("Parameter 'managedIdentity' is not allowed if 'accountKey' is provided",
                        () -> builder.build(config("endpoint", AZURE_ENDPOINT, "accountName", "a", "accountKey", "k",
                                "managedIdentity", on)));
                assertRejected("Parameter 'tenantId' is not allowed if 'managedIdentity' is provided", () -> builder
                        .build(config("endpoint", AZURE_ENDPOINT, "managedIdentity", on, "tenantId", "t")));
            }
            for (String off : OFF) {
                assertNotNull(builder.build(config("endpoint", AZURE_ENDPOINT, "accountName", "a", "accountKey", "a2V5",
                        "managedIdentity", off)));
            }
            for (String invalid : INVALID) {
                assertInvalid("managedIdentity",
                        () -> builder.build(config("endpoint", AZURE_ENDPOINT, "managedIdentity", invalid)));
            }
        }
    }

    private interface ThrowingBuilder {
        Object build(Map<String, String> configuration) throws Exception;
    }

    @Test
    public void azureHadoopManagedIdentity() throws Exception {
        for (String on : ON) {
            JobConf conf = new JobConf(false);
            AzureUtils.configureAzureHdfsJobConf(conf, config("managedIdentity", on), "a.blob.core.windows.net");
            assertEquals(AzureConstants.HADOOP_MANAGED_IDENTITY_AUTH_TYPE,
                    conf.get(AzureConstants.HADOOP_OAUTH_PROVIDER_TYPE));
        }
        for (String off : OFF) {
            JobConf conf = new JobConf(false);
            AzureUtils.configureAzureHdfsJobConf(conf, config("managedIdentity", off), "a.blob.core.windows.net");
            assertNull(conf.get(AzureConstants.HADOOP_OAUTH_PROVIDER_TYPE));
        }
        for (String invalid : INVALID) {
            assertInvalid("managedIdentity", () -> AzureUtils.configureAzureHdfsJobConf(new JobConf(false),
                    config("managedIdentity", invalid), "a.blob.core.windows.net"));
        }
    }
}
