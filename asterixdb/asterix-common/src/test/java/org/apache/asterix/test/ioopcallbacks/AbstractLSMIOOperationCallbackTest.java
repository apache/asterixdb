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

        //request to flush first component
        callback.updateLastLSN(1);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        //request to flush second component
        callback.updateLastLSN(2);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        Assert.assertEquals(1, callback.getComponentLSN(null));
        final ILSMDiskComponent diskComponent1 = mockDiskComponent();
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent1);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent1);

        Assert.assertEquals(2, callback.getComponentLSN(null));
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent2);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent2);
    }

    @Test
    public void testOverWrittenLSN() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));

        LSMBTreeIOOperationCallback callback = new LSMBTreeIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());

        //request to flush first component
        callback.updateLastLSN(1);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        //request to flush second component
        callback.updateLastLSN(2);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        //request to flush first component again
        //this call should fail
        callback.updateLastLSN(3);
        //there is no corresponding beforeOperation, since the first component is being flush
        //the scheduleFlush request would fail this time

        Assert.assertEquals(1, callback.getComponentLSN(null));
        final ILSMDiskComponent diskComponent1 = mockDiskComponent();
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent1);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent1);
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        Assert.assertEquals(2, callback.getComponentLSN(null));
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent2);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent2);
    }

    @Test
    public void testLostLSN() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        Mockito.when(mockIndex.getCurrentMemoryComponent()).thenReturn(Mockito.mock(AbstractLSMMemoryComponent.class));

        LSMBTreeIOOperationCallback callback = new LSMBTreeIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());
        //request to flush first component
        callback.updateLastLSN(1);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        //request to flush second component
        callback.updateLastLSN(2);
        callback.beforeOperation(LSMIOOperationType.FLUSH);

        Assert.assertEquals(1, callback.getComponentLSN(null));

        // the first flush is finished, but has not finalized yet (in codebase, these two calls
        // are not synchronized)
        callback.afterOperation(LSMIOOperationType.FLUSH, null, mockDiskComponent());

        //request to flush first component again
        callback.updateLastLSN(3);

        // the first flush is finalized (it may be called after afterOperation for a while)
        callback.afterFinalize(LSMIOOperationType.FLUSH, mockDiskComponent());

        // the second flush gets LSN 2
        Assert.assertEquals(2, callback.getComponentLSN(null));
        // the second flush is finished
        callback.afterOperation(LSMIOOperationType.FLUSH, null, mockDiskComponent());
        callback.afterFinalize(LSMIOOperationType.FLUSH, mockDiskComponent());

        // it should get new LSN 3
        Assert.assertEquals(3, callback.getComponentLSN(null));
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
            callback.beforeOperation(LSMIOOperationType.FLUSH);
            callback.recycled(mockComponent, true);

            final ILSMDiskComponent diskComponent = mockDiskComponent();
            callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent);
            callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent);
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
            callback.recycled(mockComponent, false);
            callback.afterFinalize(LSMIOOperationType.FLUSH, null);
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
        callback.beforeOperation(LSMIOOperationType.FLUSH);
        final ILSMDiskComponent diskComponent = mockDiskComponent();
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent);

        // another flush is to be scheduled before the component is recycled
        idGenerator.refresh();
        ILSMComponentId nextId = idGenerator.getId();

        // recycle the component
        callback.recycled(mockComponent, true);
        checkMemoryComponent(expectedId, mockComponent);

        // schedule the next flush
        callback.updateLastLSN(0);
        callback.beforeOperation(LSMIOOperationType.FLUSH);
        final ILSMDiskComponent diskComponent2 = mockDiskComponent();
        callback.afterOperation(LSMIOOperationType.FLUSH, null, diskComponent2);
        callback.afterFinalize(LSMIOOperationType.FLUSH, diskComponent2);
        callback.recycled(mockComponent, true);
        checkMemoryComponent(nextId, mockComponent);
    }

    private void checkMemoryComponent(ILSMComponentId expected, ILSMMemoryComponent memoryComponent)
            throws HyracksDataException {
        ArgumentCaptor<ILSMComponentId> argument = ArgumentCaptor.forClass(ILSMComponentId.class);
        Mockito.verify(memoryComponent).resetId(argument.capture());
        assertEquals(expected, argument.getValue());

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
