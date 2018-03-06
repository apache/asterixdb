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

import java.util.Collections;

import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallback;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentIdGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import junit.framework.TestCase;

public abstract class AbstractLSMIOOperationCallbackTest extends TestCase {

    @Test
    public void testNormalSequence() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));
        LSMBTreeIOOperationCallback callback = new LSMBTreeIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());
        ILSMIndexOperationContext firstOpCtx = new TestLSMIndexOperationContext(mockIndex);

        //request to flush first component
        callback.updateLastLSN(1);
        firstOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(firstOpCtx);

        ILSMIndexOperationContext secondOpCtx = new TestLSMIndexOperationContext(mockIndex);
        //request to flush second component
        callback.updateLastLSN(2);
        secondOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(secondOpCtx);

        Assert.assertEquals(1, callback.getComponentLSN(Collections.emptyList()));
        final ILSMDiskComponent diskComponent1 = mockDiskComponent();
        firstOpCtx.setNewComponent(diskComponent1);
        callback.afterOperation(firstOpCtx);
        callback.afterFinalize(firstOpCtx);

        Assert.assertEquals(2, callback.getComponentLSN(Collections.emptyList()));
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        secondOpCtx.setNewComponent(diskComponent2);
        callback.afterOperation(secondOpCtx);
        callback.afterFinalize(secondOpCtx);
    }

    @Test
    public void testOverWrittenLSN() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));
        LSMBTreeIOOperationCallback callback = new LSMBTreeIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());

        //request to flush first component
        ILSMIndexOperationContext firstOpCtx = new TestLSMIndexOperationContext(mockIndex);
        callback.updateLastLSN(1);
        firstOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(firstOpCtx);

        //request to flush second component
        ILSMIndexOperationContext secondOpCtx = new TestLSMIndexOperationContext(mockIndex);
        callback.updateLastLSN(2);
        secondOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(secondOpCtx);

        //request to flush first component again
        //this call should fail
        callback.updateLastLSN(3);
        //there is no corresponding beforeOperation, since the first component is being flush
        //the scheduleFlush request would fail this time

        Assert.assertEquals(1, callback.getComponentLSN(Collections.emptyList()));
        final ILSMDiskComponent diskComponent1 = mockDiskComponent();
        firstOpCtx.setNewComponent(diskComponent1);
        callback.afterOperation(firstOpCtx);
        callback.afterFinalize(firstOpCtx);
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        secondOpCtx.setNewComponent(diskComponent2);
        Assert.assertEquals(2, callback.getComponentLSN(Collections.emptyList()));
        callback.afterOperation(secondOpCtx);
        callback.afterFinalize(secondOpCtx);
    }

    @Test
    public void testLostLSN() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));

        LSMBTreeIOOperationCallback callback = new LSMBTreeIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());
        //request to flush first component
        ILSMIndexOperationContext firstOpCtx = new TestLSMIndexOperationContext(mockIndex);
        callback.updateLastLSN(1);
        firstOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(firstOpCtx);

        //request to flush second component
        ILSMIndexOperationContext secondOpCtx = new TestLSMIndexOperationContext(mockIndex);
        callback.updateLastLSN(2);
        secondOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(secondOpCtx);

        Assert.assertEquals(1, callback.getComponentLSN(Collections.emptyList()));

        // the first flush is finished, but has not finalized yet (in codebase, these two calls
        // are not synchronized)
        firstOpCtx.setNewComponent(mockDiskComponent());
        callback.afterOperation(firstOpCtx);

        //request to flush first component again
        callback.updateLastLSN(3);

        // the first flush is finalized (it may be called after afterOperation for a while)
        callback.afterFinalize(firstOpCtx);

        // the second flush gets LSN 2
        Assert.assertEquals(2, callback.getComponentLSN(Collections.emptyList()));
        // the second flush is finished
        secondOpCtx.setNewComponent(mockDiskComponent());
        callback.afterOperation(secondOpCtx);
        callback.afterFinalize(secondOpCtx);

        // it should get new LSN 3
        Assert.assertEquals(3, callback.getComponentLSN(Collections.emptyList()));
    }

    @Test
    public void testAllocateComponentId() throws HyracksDataException {
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator();
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);

        LSMBTreeIOOperationCallback callback =
                new LSMBTreeIOOperationCallback(mockIndex, idGenerator, mockIndexCheckpointManagerProvider());

        ILSMComponentId initialId = idGenerator.getId();
        // simulate a partition is flushed before allocated
        idGenerator.refresh();
        callback.updateLastLSN(0);

        callback.allocated(mockComponent);
        checkMemoryComponent(initialId, mockComponent);
    }

    @Test
    public void testRecycleComponentId() throws HyracksDataException {
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator();
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);
        LSMBTreeIOOperationCallback callback =
                new LSMBTreeIOOperationCallback(mockIndex, idGenerator, mockIndexCheckpointManagerProvider());

        ILSMComponentId id = idGenerator.getId();
        callback.allocated(mockComponent);
        checkMemoryComponent(id, mockComponent);

        Mockito.when(mockIndex.isMemoryComponentsAllocated()).thenReturn(true);
        for (int i = 0; i < 100; i++) {
            // schedule a flush
            idGenerator.refresh();
            ILSMComponentId expectedId = idGenerator.getId();
            callback.updateLastLSN(0);
            ILSMIndexOperationContext opCtx = new TestLSMIndexOperationContext(mockIndex);
            opCtx.setIoOperationType(LSMIOOperationType.FLUSH);
            callback.beforeOperation(opCtx);
            callback.recycled(mockComponent, true);
            opCtx.setNewComponent(mockDiskComponent());
            callback.afterOperation(opCtx);
            callback.afterFinalize(opCtx);
            checkMemoryComponent(expectedId, mockComponent);
        }
    }

    @Test
    public void testRecycleWithoutSwitch() throws HyracksDataException {
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator();
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);
        LSMBTreeIOOperationCallback callback =
                new LSMBTreeIOOperationCallback(mockIndex, idGenerator, mockIndexCheckpointManagerProvider());
        ILSMComponentId id = idGenerator.getId();
        callback.allocated(mockComponent);
        checkMemoryComponent(id, mockComponent);
        Mockito.when(mockIndex.isMemoryComponentsAllocated()).thenReturn(true);
        for (int i = 0; i < 10; i++) {
            idGenerator.refresh();
            id = idGenerator.getId();
            callback.updateLastLSN(0);
            // Huh! There is no beforeOperation?
            ILSMIndexOperationContext opCtx = new TestLSMIndexOperationContext(mockIndex);
            opCtx.setIoOperationType(LSMIOOperationType.FLUSH);
            callback.recycled(mockComponent, false);
            callback.afterFinalize(opCtx);
            checkMemoryComponent(id, mockComponent);
        }
    }

    @Test
    public void testConcurrentRecycleComponentId() throws HyracksDataException {
        ILSMComponentIdGenerator idGenerator = new LSMComponentIdGenerator();
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        ILSMMemoryComponent mockComponent = Mockito.mock(AbstractLSMMemoryComponent.class);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(mockComponent);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        LSMBTreeIOOperationCallback callback =
                new LSMBTreeIOOperationCallback(mockIndex, idGenerator, mockIndexCheckpointManagerProvider());

        ILSMComponentId id = idGenerator.getId();
        callback.allocated(mockComponent);
        checkMemoryComponent(id, mockComponent);

        Mockito.when(mockIndex.isMemoryComponentsAllocated()).thenReturn(true);

        // schedule a flush
        idGenerator.refresh();
        ILSMComponentId expectedId = idGenerator.getId();

        callback.updateLastLSN(0);
        ILSMIndexOperationContext firstOpCtx = new TestLSMIndexOperationContext(mockIndex);
        firstOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(firstOpCtx);
        firstOpCtx.setNewComponent(mockDiskComponent());
        callback.afterOperation(firstOpCtx);
        callback.afterFinalize(firstOpCtx);

        // another flush is to be scheduled before the component is recycled
        idGenerator.refresh();
        ILSMComponentId nextId = idGenerator.getId();

        // recycle the component
        callback.recycled(mockComponent, true);
        checkMemoryComponent(expectedId, mockComponent);

        // schedule the next flush
        callback.updateLastLSN(0);
        ILSMIndexOperationContext secondOpCtx = new TestLSMIndexOperationContext(mockIndex);
        secondOpCtx.setIoOperationType(LSMIOOperationType.FLUSH);
        callback.beforeOperation(secondOpCtx);
        secondOpCtx.setNewComponent(mockDiskComponent());
        callback.afterOperation(secondOpCtx);
        callback.afterFinalize(secondOpCtx);
        callback.recycled(mockComponent, true);
        checkMemoryComponent(nextId, mockComponent);
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
        Mockito.doNothing().when(indexCheckpointManager).flushed(Mockito.any(), Mockito.anyLong());
        Mockito.doReturn(indexCheckpointManager).when(indexCheckpointManagerProvider).get(Mockito.any());
        return indexCheckpointManagerProvider;
    }

    protected abstract AbstractLSMIOOperationCallback getIoCallback() throws HyracksDataException;
}
