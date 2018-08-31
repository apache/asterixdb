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

package org.apache.asterix.test.ioopcallbacks;

import static org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId.MIN_VALID_COMPONENT_ID;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentIdGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import junit.framework.TestCase;

public class LSMIOOperationCallbackTest extends TestCase {
    /*
     * The normal sequence of calls:
     * 1. refresh id generator
     * 2. flushLsn
     * 3. created
     * 4. before
     * 5. after
     * 6. finalize
     * 7. destroy
     */

    private static long COMPONENT_SEQUENCE = 0;

    private static String getComponentFileName() {
        final String sequence = String.valueOf(COMPONENT_SEQUENCE++);
        return sequence + '_' + sequence;
    }

    @Test
    public void testNormalSequence() throws HyracksDataException {
        int numMemoryComponents = 2;

        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        String indexId = "mockIndexId";
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(numMemoryComponents);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));
        DatasetInfo dsInfo = new DatasetInfo(101, null);
        LSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator(numMemoryComponents, MIN_VALID_COMPONENT_ID);
        LSMIOOperationCallback callback = new LSMIOOperationCallback(dsInfo, mockIndex, idGenerator.getId(),
                mockIndexCheckpointManagerProvider());
        //Flush first
        idGenerator.refresh();
        long flushLsn = 1L;
        ILSMComponentId nextComponentId = idGenerator.getId();
        Map<String, Object> flushMap = new HashMap<>();
        flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
        flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, nextComponentId);
        ILSMIndexAccessor firstAccessor = new TestLSMIndexAccessor(new TestLSMIndexOperationContext(mockIndex));
        firstAccessor.getOpContext().setParameters(flushMap);
        FileReference firstTarget = new FileReference(Mockito.mock(IODeviceHandle.class), getComponentFileName());
        LSMComponentFileReferences firstFiles = new LSMComponentFileReferences(firstTarget, firstTarget, firstTarget);
        FlushOperation firstFlush = new TestFlushOperation(firstAccessor, firstTarget, callback, indexId, firstFiles,
                new LSMComponentId(0, 0));
        callback.scheduled(firstFlush);
        callback.beforeOperation(firstFlush);

        //Flush second
        idGenerator.refresh();
        flushLsn = 2L;
        nextComponentId = idGenerator.getId();
        flushMap = new HashMap<>();
        flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
        flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, nextComponentId);
        ILSMIndexAccessor secondAccessor = new TestLSMIndexAccessor(new TestLSMIndexOperationContext(mockIndex));
        secondAccessor.getOpContext().setParameters(flushMap);
        FileReference secondTarget = new FileReference(Mockito.mock(IODeviceHandle.class), getComponentFileName());
        LSMComponentFileReferences secondFiles =
                new LSMComponentFileReferences(secondTarget, secondTarget, secondTarget);
        FlushOperation secondFlush = new TestFlushOperation(secondAccessor, secondTarget, callback, indexId,
                secondFiles, new LSMComponentId(1, 1));
        callback.scheduled(secondFlush);
        callback.beforeOperation(secondFlush);

        Map<String, Object> firstFlushMap = firstFlush.getAccessor().getOpContext().getParameters();
        long firstFlushLogLsn = (Long) firstFlushMap.get(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN);
        Assert.assertEquals(1, firstFlushLogLsn);
        final ILSMDiskComponent diskComponent1 = mockDiskComponent();
        firstFlush.setNewComponent(diskComponent1);
        callback.afterOperation(firstFlush);
        callback.afterFinalize(firstFlush);
        callback.completed(firstFlush);

        Map<String, Object> secondFlushMap = secondFlush.getAccessor().getOpContext().getParameters();
        long secondFlushLogLsn = (Long) secondFlushMap.get(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN);
        Assert.assertEquals(2, secondFlushLogLsn);
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        secondFlush.setNewComponent(diskComponent2);
        callback.afterOperation(secondFlush);
        callback.afterFinalize(secondFlush);
        callback.completed(secondFlush);
    }

    @Test
    public void testAllocateComponentId() throws HyracksDataException {
        int numMemoryComponents = 2;
        DatasetInfo dsInfo = new DatasetInfo(101, null);
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator(numMemoryComponents, MIN_VALID_COMPONENT_ID);
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(numMemoryComponents);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);
        LSMIOOperationCallback callback = new LSMIOOperationCallback(dsInfo, mockIndex, idGenerator.getId(),
                mockIndexCheckpointManagerProvider());
        ILSMComponentId initialId = idGenerator.getId();
        // simulate a partition is flushed before allocated
        idGenerator.refresh();
        long flushLsn = 1L;
        ILSMComponentId nextComponentId = idGenerator.getId();
        callback.allocated(mockComponent);
        callback.recycled(mockComponent);
        checkMemoryComponent(initialId, mockComponent);
    }

    @Test
    public void testRecycleComponentId() throws HyracksDataException {
        int numMemoryComponents = 2;
        DatasetInfo dsInfo = new DatasetInfo(101, null);
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator(numMemoryComponents, MIN_VALID_COMPONENT_ID);
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(numMemoryComponents);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);
        LSMIOOperationCallback callback = new LSMIOOperationCallback(dsInfo, mockIndex, idGenerator.getId(),
                mockIndexCheckpointManagerProvider());
        String indexId = "mockIndexId";
        ILSMComponentId id = idGenerator.getId();
        callback.recycled(mockComponent);
        checkMemoryComponent(id, mockComponent);

        Mockito.when(mockIndex.isMemoryComponentsAllocated()).thenReturn(true);
        for (int i = 0; i < 100; i++) {
            // schedule a flush
            idGenerator.refresh();
            ILSMComponentId expectedId = idGenerator.getId();
            long flushLsn = 0L;
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, expectedId);
            ILSMIndexAccessor accessor = new TestLSMIndexAccessor(new TestLSMIndexOperationContext(mockIndex));
            accessor.getOpContext().setParameters(flushMap);
            FileReference target = new FileReference(Mockito.mock(IODeviceHandle.class), getComponentFileName());
            LSMComponentFileReferences files = new LSMComponentFileReferences(target, target, target);
            FlushOperation flush =
                    new TestFlushOperation(accessor, target, callback, indexId, files, new LSMComponentId(0, 0));
            callback.scheduled(flush);
            callback.beforeOperation(flush);
            callback.recycled(mockComponent);
            flush.setNewComponent(mockDiskComponent());
            callback.afterOperation(flush);
            callback.afterFinalize(flush);
            callback.completed(flush);
            checkMemoryComponent(expectedId, mockComponent);
        }
    }

    private void checkMemoryComponent(ILSMComponentId expected, ILSMMemoryComponent memoryComponent)
            throws HyracksDataException {
        ArgumentCaptor<ILSMComponentId> idArgument = ArgumentCaptor.forClass(ILSMComponentId.class);
        ArgumentCaptor<Boolean> forceArgument = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(memoryComponent).resetId(idArgument.capture(), forceArgument.capture());
        assertEquals(expected, idArgument.getValue());
        assertEquals(false, forceArgument.getValue().booleanValue());
        Mockito.reset(memoryComponent);
    }

    private ILSMDiskComponent mockDiskComponent() {
        ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
        Mockito.when(component.getMetadata()).thenReturn(Mockito.mock(DiskComponentMetadata.class));
        return component;
    }

    protected IIndexCheckpointManagerProvider mockIndexCheckpointManagerProvider() throws HyracksDataException {
        IIndexCheckpointManagerProvider indexCheckpointManagerProvider =
                Mockito.mock(IIndexCheckpointManagerProvider.class);
        IIndexCheckpointManager indexCheckpointManager = Mockito.mock(IIndexCheckpointManager.class);
        Mockito.doNothing().when(indexCheckpointManager).flushed(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.anyLong());
        Mockito.doReturn(indexCheckpointManager).when(indexCheckpointManagerProvider).get(Mockito.any());
        return indexCheckpointManagerProvider;
    }
}
