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
package org.apache.asterix.transaction.management.service.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Regression test for prepared resources lost when several partitions report concurrently")
public class GlobalTransactionContextTest {

    private static final int NUM_NODES = 8;
    private static final int PARTITIONS_PER_NODE = 4;
    private static final int NUM_PARTITIONS = NUM_NODES * PARTITIONS_PER_NODE;
    private static final int ITERATIONS = 200;

    /**
     * Every participating partition reports its prepared resources on its own CC executor thread, and all of
     * them must survive. A node missing from the map is never sent a commit message, so its partitions stay
     * uncommitted while the statement still reports success.
     */
    @Test
    public void concurrentPreparedResourcesAreNotLost() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_PARTITIONS);
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                GlobalTransactionContext context = new GlobalTransactionContext(new JobId(iteration),
                        Collections.singletonList(1), NUM_NODES, NUM_PARTITIONS);
                CyclicBarrier startLine = new CyclicBarrier(NUM_PARTITIONS);
                CountDownLatch done = new CountDownLatch(NUM_PARTITIONS);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                for (int node = 0; node < NUM_NODES; node++) {
                    for (int partition = 0; partition < PARTITIONS_PER_NODE; partition++) {
                        String nodeId = "node_" + node;
                        String resource = nodeId + "/partition_" + partition;
                        executor.submit(() -> {
                            try {
                                startLine.await();
                                context.addPreparedNodeResources(nodeId,
                                        Collections.singletonMap(resource, new LSMComponentId(1, 1)));
                            } catch (Throwable th) {
                                failure.compareAndSet(null, th);
                            } finally {
                                done.countDown();
                            }
                        });
                    }
                }
                assertTrue("timed out waiting for the partitions to report", done.await(30, TimeUnit.SECONDS));
                if (failure.get() != null) {
                    throw new AssertionError("reporting a prepared partition failed", failure.get());
                }
                Map<String, Map<String, ILSMComponentId>> nodeResourceMap = context.getNodeResourceMap();
                assertEquals("iteration " + iteration + ": a node was dropped from the commit broadcast", NUM_NODES,
                        nodeResourceMap.size());
                for (Map.Entry<String, Map<String, ILSMComponentId>> entry : nodeResourceMap.entrySet()) {
                    assertEquals("iteration " + iteration + ": " + entry.getKey() + " lost a partition",
                            PARTITIONS_PER_NODE, entry.getValue().size());
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
