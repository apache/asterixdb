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
package org.apache.hyracks.api.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.After;
import org.junit.Test;

@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Contract of the shared interrupt poll")
public class InvokeUtilFailIfInterruptedTest {

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    public void returnsQuietlyWhenNotInterrupted() throws Exception {
        Thread.interrupted();
        InvokeUtil.failIfInterrupted();
    }

    @Test
    public void throwsAndPreservesInterruptStatus() {
        Thread.currentThread().interrupt();
        try {
            InvokeUtil.failIfInterrupted();
            fail("expected the interrupt to be reported");
        } catch (HyracksDataException e) {
            // task teardown reads the status after unwinding, so consuming it here would lose the cancellation
            assertTrue("interrupt status was consumed", Thread.currentThread().isInterrupted());
            assertTrue("not recognized as an interrupt: " + e, ExceptionUtils.causedByInterrupt(e));
        }
    }
}
