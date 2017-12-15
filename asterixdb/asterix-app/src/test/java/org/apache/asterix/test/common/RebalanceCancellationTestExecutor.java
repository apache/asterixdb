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

package org.apache.asterix.test.common;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ComparisonEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.logging.log4j.Level;
import org.junit.Assert;

public class RebalanceCancellationTestExecutor extends TestExecutor {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private long waitTime = 100;

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    @Override
    protected void executeHttpRequest(TestCaseContext.OutputFormat fmt, String statement,
            Map<String, Object> variableCtx, String reqType, File testFile, File expectedResultFile,
            File actualResultFile, MutableInt queryCount, int numResultFiles, String extension, ComparisonEnum compare)
            throws Exception {
        // Executes regular tests as usual.
        if (!(testFile.getAbsolutePath().endsWith("post.http") && statement.contains("rebalance"))) {
            super.executeHttpRequest(fmt, statement, variableCtx, reqType, testFile, expectedResultFile,
                    actualResultFile, queryCount, numResultFiles, extension, compare);
            return;
        }

        // Executes rebalance tests with cancellation.
        Future<Exception> future = executor.submit(() -> {
            //boolean failed = false;
            try {
                super.executeHttpRequest(fmt, statement, variableCtx, reqType, testFile, expectedResultFile,
                        actualResultFile, queryCount, numResultFiles, extension, compare);
            } catch (Exception e) {
                // Since Hyracks job cancellation is not synchronous, re-executing rebalance could
                // fail, but we keep retrying until it completes.
                boolean done = false;
                do {
                    try {
                        // Re-executes rebalance.
                        super.executeHttpRequest(fmt, statement, variableCtx, reqType, testFile, expectedResultFile,
                                actualResultFile, queryCount, numResultFiles, extension, compare);
                        done = true;
                    } catch (Exception e2) {
                        String errorMsg = ExceptionUtils.getErrorMessage(e2);
                        // not expected, but is a false alarm.
                        if (errorMsg == null || !errorMsg.contains("reference count = 1")) {
                            return e2;
                        }
                        LOGGER.log(Level.WARN, e2.toString(), e2);
                    }
                } while (!done);
            }
            return null;
        });
        Thread.sleep(waitTime);
        // Cancels the query request while the query is executing.
        int rc = cancelQuery(getEndpoint(Servlets.REBALANCE), Collections.emptyList());
        Assert.assertTrue(rc == 200 || rc == 404);
        Exception e = future.get();
        if (e != null) {
            throw e;
        }
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
}
