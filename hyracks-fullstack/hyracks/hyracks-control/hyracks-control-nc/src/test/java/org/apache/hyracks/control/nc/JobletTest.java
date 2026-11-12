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
package org.apache.hyracks.control.nc;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.memory.IMemoryManager;
import org.junit.Test;

public class JobletTest {

    private static final int FRAME_SIZE = 32768;

    @Test
    public void testBalancedAllocationDoesNotLeakOnClose() throws Exception {
        IMemoryManager memoryManager = mock(IMemoryManager.class);
        when(memoryManager.allocate(FRAME_SIZE)).thenReturn(true);
        Joblet joblet = createJoblet(memoryManager);

        joblet.allocateFrame(FRAME_SIZE);
        joblet.deallocateFrames(FRAME_SIZE);
        joblet.close();

        verify(memoryManager).allocate(FRAME_SIZE);
        verify(memoryManager, times(1)).deallocate(FRAME_SIZE);
        verifyNoMoreInteractions(memoryManager);
    }

    @Test
    public void testFailedAllocationReleasesReservedMemoryImmediately() throws Exception {
        int invalidSize = FRAME_SIZE + 1;
        IMemoryManager memoryManager = mock(IMemoryManager.class);
        when(memoryManager.allocate(invalidSize)).thenReturn(true);
        Joblet joblet = createJoblet(memoryManager);

        try {
            joblet.allocateFrame(invalidSize);
            fail("Expected frame allocation to fail for a non-integral frame size");
        } catch (HyracksDataException e) {
            // expected
        }

        verify(memoryManager).allocate(invalidSize);
        verify(memoryManager).deallocate(invalidSize);

        joblet.close();

        verifyNoMoreInteractions(memoryManager);
    }

    private static Joblet createJoblet(IMemoryManager memoryManager) {
        NodeControllerService nodeController = mock(NodeControllerService.class);
        ExecutorService executor = mock(ExecutorService.class);
        when(nodeController.getId()).thenReturn("nc1");
        when(nodeController.getExecutor()).thenReturn(executor);

        INCServiceContext serviceContext = mock(INCServiceContext.class);
        when(serviceContext.getMemoryManager()).thenReturn(memoryManager);
        when(serviceContext.getIoManager()).thenReturn(mock(IIOManager.class));

        ActivityClusterGraph activityClusterGraph = new ActivityClusterGraph();
        activityClusterGraph.setFrameSize(FRAME_SIZE);
        return new Joblet(nodeController, null, new JobId(1), serviceContext, activityClusterGraph, null, 0L, "UTC");
    }
}
