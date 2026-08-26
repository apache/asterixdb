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
package org.apache.hyracks.control.common.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;

import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobKind;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StartTasksFunction;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class StartTasksFunctionSerDeTest {

    private static final String TZ = "UTC";

    @Test
    public void jobKindRoundTrips() throws Exception {
        for (JobKind kind : JobKind.values()) {
            assertEquals(kind, deserialize(serialize(newFunction(kind))).getJobKind());
        }
    }

    @Test
    public void nullJobKindRoundTrips() throws Exception {
        assertNull(deserialize(serialize(newFunction(null))).getJobKind());
    }

    private static StartTasksFunction newFunction(JobKind kind) {
        return new StartTasksFunction(null, new JobId(1), null, Collections.emptyList(), Collections.emptyMap(),
                EnumSet.noneOf(JobFlag.class), Collections.emptyMap(), null, 0L, TZ, kind);
    }

    private static byte[] serialize(StartTasksFunction fn) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StartTasksFunction.serialize(baos, fn);
        return baos.toByteArray();
    }

    private static StartTasksFunction deserialize(byte[] bytes) throws Exception {
        return (StartTasksFunction) StartTasksFunction.deserialize(ByteBuffer.wrap(bytes), bytes.length);
    }
}
