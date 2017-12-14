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

package org.apache.asterix.test.runtime;

import java.util.Collection;

import org.apache.asterix.test.common.RebalanceCancellationTestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs rebalance tests with cancellation.
 */
@RunWith(Parameterized.class)
public class RebalanceWithCancellationIT {
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static RebalanceCancellationTestExecutor executor = new RebalanceCancellationTestExecutor();

    @BeforeClass
    public static void setUp() throws Exception {
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, executor);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "RebalanceWithCancellationIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "rebalance.xml");
    }

    protected TestCaseContext tcCtx;

    public RebalanceWithCancellationIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        // Runs each single cancellation test multiple times and tests cancellation at various points of time.
        for (int waitTime = 100; waitTime <= 1000; waitTime += 50) {
            executor.setWaitTime(waitTime);
            LangExecutionUtil.test(tcCtx);
        }
    }
}
