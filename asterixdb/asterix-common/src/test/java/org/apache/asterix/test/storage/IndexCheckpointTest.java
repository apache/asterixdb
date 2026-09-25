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
package org.apache.asterix.test.storage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

public class IndexCheckpointTest {

    /**
     * The pending-creation flag must survive both a checkpoint advance and a round trip through the checkpoint file,
     * or an index whose creation died is taken for a complete one and kept (ASTERIXDB-3839).
     */
    @Test
    public void pendingCreationSurvivesFlushAndSerialization() throws HyracksDataException {
        IndexCheckpoint pending = IndexCheckpoint.first(0, 0, 0, null, true);
        assertTrue(pending.isPendingCreation());

        // a component flushed by the load itself must not clear it; only the creator's last step does
        IndexCheckpoint afterFlush = IndexCheckpoint.next(pending, 1, 1, 1, null, pending.isPendingCreation());
        assertTrue(afterFlush.isPendingCreation());
        assertTrue(IndexCheckpoint.fromJson(afterFlush.asJson()).isPendingCreation());

        IndexCheckpoint completed = IndexCheckpoint.next(afterFlush, afterFlush.getLowWatermark(),
                afterFlush.getValidComponentSequence(), afterFlush.getLastComponentId(), null, false);
        assertFalse(completed.isPendingCreation());
        assertFalse(IndexCheckpoint.fromJson(completed.asJson()).isPendingCreation());
        assertFalse(IndexCheckpoint.next(completed, 2, 2, 2, null, completed.isPendingCreation()).isPendingCreation());
    }

    /** A checkpoint written before the flag existed reads as complete. */
    @Test
    public void legacyCheckpointIsNotPending() throws HyracksDataException {
        String legacy = "{\"id\":0,\"validComponentSequence\":0,\"lowWatermark\":0,\"lastComponentId\":0,"
                + "\"masterNodeFlushMap\":{},\"masterNodeId\":null,\"masterValidSeq\":0}";
        assertFalse(IndexCheckpoint.fromJson(legacy).isPendingCreation());
    }
}
