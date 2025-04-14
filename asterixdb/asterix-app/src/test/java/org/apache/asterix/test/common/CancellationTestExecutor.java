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

import static org.apache.asterix.api.http.server.QueryServiceRequestParameters.Parameter.CLIENT_ID;
import static org.apache.asterix.test.common.ComparisonException.Type.DIFFERENT_RESULT;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.runtime.SqlppExecutionWithCancellationTest;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.HttpResponse;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.junit.Assert;

public class CancellationTestExecutor extends TestExecutor {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public CancellationTestExecutor() {
        super();
    }

    public CancellationTestExecutor(String deltaPath) {
        super(deltaPath);
    }

    @Override
    public InputStream executeQueryService(String str, TestCaseContext.OutputFormat fmt, URI uri,
            List<TestCase.CompilationUnit.Parameter> params, List<TestCase.CompilationUnit.Placeholder> placeholders,
            boolean jsonEncoded, Charset responseCharset, Predicate<Integer> responseCodeValidator, boolean cancellable)
            throws Exception {
        cancellable = cancellable && !containsClientContextID(str);
        String clientContextId = UUID.randomUUID().toString();
        final List<TestCase.CompilationUnit.Parameter> newParams =
                cancellable ? upsertParam(params, CLIENT_ID.str(), ParameterTypeEnum.STRING, clientContextId) : params;
        Callable<InputStream> query = () -> {
            try {
                return CancellationTestExecutor.super.executeQueryService(str, fmt, uri,
                        constructQueryParameters(str, fmt, newParams), placeholders, jsonEncoded, responseCharset,
                        responseCodeValidator, true);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        };
        Future<InputStream> future = executor.submit(query);
        while (!future.isDone()) {
            if (cancellable) {
                Thread.sleep(10);
                // Cancels the query request while the query is executing.
                int rc = cancelQuery(getEndpoint(Servlets.RUNNING_REQUESTS), newParams);
                Assert.assertTrue(rc == 200 || rc == 404 || rc == 403);
                if (rc == 200) {
                    break;
                }
            }
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
        HttpResponse response = executeHttpRequest(constructDeleteMethod(uri, params));
        return response.getStatusLine().getStatusCode();
    }

    @Override
    protected boolean isUnExpected(Exception e, List<String> expectedErrors, int numOfErrors, MutableInt queryCount,
            boolean expectedSourceLoc) {
        // Get the expected exception
        for (Iterator<String> iter = expectedErrors.iterator(); iter.hasNext();) {
            String expectedError = iter.next();
            if (e.toString().contains(expectedError)) {
                System.err.println("...but that was expected.");
                iter.remove();
                return false;
            }
        }
        String errorMsg = ExceptionUtils.getErrorMessage(e);
        // Expected, "HYR0025" or "ASX0041" means a user cancelled the query.)
        if (errorMsg.startsWith("HYR0025") || errorMsg.startsWith("ASX0041")) {
            SqlppExecutionWithCancellationTest.numCancelledQueries++;
            queryCount.increment();
            return false;
        } else if (e instanceof ComparisonException
                && ((ComparisonException) e).getExceptionType() == DIFFERENT_RESULT) {
            // for this test, ignore ComparisonException for completed requests since the goal is not to compare results
            return false;
        } else {
            System.err.println(
                    "Expected to find one of the following in error text:\n+++++\n" + expectedErrors + "\n+++++");
            return true;
        }
    }

    @Override
    protected void ensureWarnings(BitSet expectedWarnings, TestCase.CompilationUnit cUnit) throws Exception {
        // skip checking warnings as currently cancelled queries with warnings might not run successfully at all
    }
}
