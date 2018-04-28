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

package org.apache.asterix.test.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicy;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.LocalResource;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import junit.framework.TestCase;

public class CorrelatedPrefixMergePolicyTest extends TestCase {

    private final long DEFAULT_COMPONENT_SIZE = 1L;

    private final int MAX_COMPONENT_SIZE = 3;

    private final int MAX_COMPONENT_COUNT = 3;

    private final int DATASET_ID = 1;

    private long nextResourceId = 0;

    @Test
    public void testBasic() {
        try {
            List<ILSMComponentId> componentIDs = Arrays.asList(new LSMComponentId(5, 5), new LSMComponentId(4, 4),
                    new LSMComponentId(3, 3), new LSMComponentId(2, 2), new LSMComponentId(1, 1));

            List<ILSMComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMComponentId(4, 4), new LSMComponentId(3, 3),
                    new LSMComponentId(2, 2), new LSMComponentId(1, 1)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMComponentId(4, 4), new LSMComponentId(3, 3),
                    new LSMComponentId(2, 2), new LSMComponentId(1, 1)), resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testIDIntervals() {
        try {
            List<ILSMComponentId> componentIDs = Arrays.asList(new LSMComponentId(40, 50), new LSMComponentId(30, 35),
                    new LSMComponentId(25, 29), new LSMComponentId(20, 24), new LSMComponentId(10, 19));

            List<ILSMComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                    new LSMComponentId(20, 24), new LSMComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                    new LSMComponentId(20, 24), new LSMComponentId(10, 19)), resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSecondaryMissing() {
        try {
            List<ILSMComponentId> primaryComponentIDs =
                    Arrays.asList(new LSMComponentId(40, 50), new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                            new LSMComponentId(20, 24), new LSMComponentId(10, 19));
            List<ILSMComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, primaryComponentIDs, resultPrimaryIDs, 0);

            List<ILSMComponentId> secondaryComponentIDs =
                    Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29), new LSMComponentId(20, 24));
            List<ILSMComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, secondaryComponentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);

            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);
            Assert.assertEquals(Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                    new LSMComponentId(20, 24), new LSMComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(
                    Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29), new LSMComponentId(20, 24)),
                    resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMultiPartition() {
        try {
            List<ILSMComponentId> componentIDs = Arrays.asList(new LSMComponentId(40, 50), new LSMComponentId(30, 35),
                    new LSMComponentId(25, 29), new LSMComponentId(20, 24), new LSMComponentId(10, 19));

            List<ILSMComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            List<ILSMComponentId> resultSecondaryIDs1 = new ArrayList<>();
            IndexInfo secondary1 = mockIndex(false, componentIDs, resultSecondaryIDs, 1);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary, secondary1);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                    new LSMComponentId(20, 24), new LSMComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMComponentId(30, 35), new LSMComponentId(25, 29),
                    new LSMComponentId(20, 24), new LSMComponentId(10, 19)), resultSecondaryIDs);
            Assert.assertTrue(resultSecondaryIDs1.isEmpty());
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    private ILSMMergePolicy mockMergePolicy(IndexInfo... indexInfos) {
        Map<String, String> properties = new HashMap<>();
        properties.put("max-tolerance-component-count", String.valueOf(MAX_COMPONENT_COUNT));
        properties.put("max-mergable-component-size", String.valueOf(MAX_COMPONENT_SIZE));

        DatasetInfo dsInfo = new DatasetInfo(DATASET_ID, null);
        for (IndexInfo index : indexInfos) {
            dsInfo.addIndex(index.getResourceId(), index);
        }
        IDatasetLifecycleManager manager = Mockito.mock(IDatasetLifecycleManager.class);
        Mockito.when(manager.getDatasetInfo(DATASET_ID)).thenReturn(dsInfo);

        ILSMMergePolicy policy = new CorrelatedPrefixMergePolicy(manager, DATASET_ID);
        policy.configure(properties);
        return policy;
    }

    private IndexInfo mockIndex(boolean isPrimary, List<ILSMComponentId> componentIDs,
            List<ILSMComponentId> resultComponentIDs, int partition) throws HyracksDataException {
        List<ILSMDiskComponent> components = new ArrayList<>();
        for (ILSMComponentId id : componentIDs) {
            ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
            Mockito.when(component.getId()).thenReturn(id);
            Mockito.when(component.getComponentSize()).thenReturn(DEFAULT_COMPONENT_SIZE);
            Mockito.when(component.getState()).thenReturn(ComponentState.READABLE_UNWRITABLE);
            components.add(component);
        }

        ILSMIndex index = Mockito.mock(ILSMIndex.class);
        Mockito.when(index.getDiskComponents()).thenReturn(components);

        ILSMIndexAccessor accessor = Mockito.mock(ILSMIndexAccessor.class);
        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                List<ILSMDiskComponent> mergedComponents = invocation.getArgumentAt(0, List.class);
                mergedComponents.forEach(component -> {
                    try {
                        resultComponentIDs.add(component.getId());
                    } catch (HyracksDataException e) {
                        e.printStackTrace();
                    }
                });
                return null;
            }
        }).when(accessor).scheduleMerge(Mockito.anyListOf(ILSMDiskComponent.class));

        Mockito.when(index.createAccessor(Mockito.any(IIndexAccessParameters.class))).thenReturn(accessor);
        Mockito.when(index.isPrimaryIndex()).thenReturn(isPrimary);
        if (isPrimary) {
            PrimaryIndexOperationTracker opTracker = Mockito.mock(PrimaryIndexOperationTracker.class);
            Mockito.when(opTracker.getPartition()).thenReturn(partition);
            Mockito.when(index.getOperationTracker()).thenReturn(opTracker);
        }
        final LocalResource localResource = Mockito.mock(LocalResource.class);
        Mockito.when(localResource.getId()).thenReturn(nextResourceId++);
        IndexInfo indexInfo = new IndexInfo(index, DATASET_ID, localResource, partition);
        indexInfo.setOpen(true);
        return indexInfo;
    }

}
