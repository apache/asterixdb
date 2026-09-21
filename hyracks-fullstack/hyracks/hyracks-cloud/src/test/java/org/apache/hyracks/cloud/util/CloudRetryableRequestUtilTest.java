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
package org.apache.hyracks.cloud.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.io.request.ICloudRequest;
import org.junit.Assert;
import org.junit.Test;

import com.azure.core.exception.AzureException;

import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Asserts that a failure raised by a cloud SDK enters the retry loop instead of propagating on the first attempt.
 * Azure was previously absent from {@link CloudRetryableRequestUtil}'s catch clause, so an Azure failure aborted the
 * operation immediately while the equivalent AWS failure was retried.
 */
public class CloudRetryableRequestUtilTest {

    @Test
    public void azureFailureIsRetried() throws HyracksDataException {
        assertRetriedOnce(() -> new AzureException("simulated Azure failure"));
    }

    @Test
    public void awsFailureIsRetried() throws HyracksDataException {
        assertRetriedOnce(() -> SdkServiceException.builder().statusCode(503).message("simulated AWS failure").build());
    }

    /**
     * Fails the first attempt only, so the assertion costs a single minimum-length backoff rather than the full
     * retry budget. A failure type the retry loop does not catch propagates out of the call and fails the test.
     */
    private static void assertRetriedOnce(Supplier<RuntimeException> failure) throws HyracksDataException {
        AtomicInteger attempts = new AtomicInteger();
        ICloudRequest request = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw failure.get();
            }
        };
        CloudRetryableRequestUtil.runWithNoRetryOnInterruption(request);
        Assert.assertEquals(2, attempts.get());
    }
}
