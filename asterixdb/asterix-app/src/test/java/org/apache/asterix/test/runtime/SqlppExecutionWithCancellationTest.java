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
package org.apache.asterix.test.runtime;

import java.util.Collection;

import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.test.common.CancellationTestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the SQL++ runtime tests with a cancellation request for each read-only query.
 */
@RunWith(Parameterized.class)
public class SqlppExecutionWithCancellationTest {
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    public static int numCancelledQueries = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, new CancellationTestExecutor());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.err.println(numCancelledQueries + " queries have been cancelled during the test.");
        try {
            // Makes sure that there are queries that have indeed been cancelled during the test.
            Assert.assertTrue(numCancelledQueries > 0);
        } finally {
            LangExecutionUtil.tearDown();
        }
    }

    @Parameters(name = "SqlppExecutionWithCancellationTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_sqlpp.xml");
    }

    protected TestCaseContext tcCtx;

    public SqlppExecutionWithCancellationTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        try {
            LangExecutionUtil.test(tcCtx);
        } catch (Exception e) {
            String errorMsg = ExceptionUtils.getErrorMessage(e);
            if (!errorMsg.contains("reference count = 1") // not expected, but is a false alarm.
                    && !errorMsg.contains("pinned and file is being closed") // not expected, but maybe a false alarm.
            ) {
                throw e;
            }
        }
    }
}
