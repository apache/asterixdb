/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.test.common;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.runtime.SqlppExecutionWithCancellationTest;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.junit.Assert;

public class CancellationTestExecutor extends TestExecutor {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public InputStream executeQueryService(String str, TestCaseContext.OutputFormat fmt, URI uri,
            List<TestCase.CompilationUnit.Parameter> params, boolean jsonEncoded, boolean cancellable)
            throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        final List<TestCase.CompilationUnit.Parameter> newParams =
                cancellable ? upsertParam(params, "client_context_id", clientContextId) : params;
        Callable<InputStream> query = () -> {
            try {
                return CancellationTestExecutor.super.executeQueryService(str, fmt, uri, newParams, jsonEncoded, true);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        };
        Future<InputStream> future = executor.submit(query);
        if (cancellable) {
            Thread.sleep(20);
            // Cancels the query request while the query is executing.
            int rc = cancelQuery(getEndpoint(Servlets.RUNNING_REQUESTS), newParams);
            Assert.assertTrue(rc == 200 || rc == 404);
        }
        InputStream inputStream = future.get();
        // Since the current cancellation (i.e., abort) implementation is based on thread.interrupt and we did not
        // track if all task threads are terminated or not, a timed wait here can reduce false alarms.
        // TODO(yingyi): investigate if we need synchronized cancellation.
        Thread.sleep(50);
        return inputStream;
    }

    // Cancels a submitted query through the cancellation REST API.
    private int cancelQuery(URI uri, List<TestCase.CompilationUnit.Parameter> params) throws Exception {
        HttpUriRequest method = constructDeleteMethodUrl(uri, params);
        HttpResponse response = executeHttpRequest(method);
        return response.getStatusLine().getStatusCode();
    }

    // Constructs a HTTP DELETE request.
    private HttpUriRequest constructDeleteMethodUrl(URI uri, List<TestCase.CompilationUnit.Parameter> otherParams) {
        RequestBuilder builder = RequestBuilder.delete(uri);
        for (TestCase.CompilationUnit.Parameter param : otherParams) {
            builder.addParameter(param.getName(), param.getValue());
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    @Override
    protected boolean isUnExpected(Exception e, CompilationUnit cUnit, int numOfErrors, MutableInt queryCount) {
        if (super.isUnExpected(e, cUnit, numOfErrors, queryCount)) {
            String errorMsg = getErrorMessage(e);
            // Expected, "HYR0025" means a user cancelled the query.)
            if (errorMsg.startsWith("HYR0025")) {
                SqlppExecutionWithCancellationTest.numCancelledQueries++;
                queryCount.increment();
            } else {
                return true;
            }
        }
        return false;
    }

    public static String getErrorMessage(Throwable th) {
        Throwable cause = getRootCause(th);
        return cause.getMessage();
    }

    // Finds the root cause of Throwable.
    private static Throwable getRootCause(Throwable e) {
        Throwable current = e;
        Throwable cause = e.getCause();
        while (cause != null && cause != current) {
            Throwable nextCause = current.getCause();
            current = cause;
            cause = nextCause;
        }
        return current;
    }
}
