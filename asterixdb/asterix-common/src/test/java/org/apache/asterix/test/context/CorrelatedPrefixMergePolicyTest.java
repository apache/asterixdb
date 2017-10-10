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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicy;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMDiskComponentId;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
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

    @Test
    public void testBasic() {
        try {
            List<ILSMDiskComponentId> componentIDs =
                    Arrays.asList(new LSMDiskComponentId(5, 5), new LSMDiskComponentId(4, 4),
                            new LSMDiskComponentId(3, 3), new LSMDiskComponentId(2, 2), new LSMDiskComponentId(1, 1));

            List<ILSMDiskComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMDiskComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(4, 4), new LSMDiskComponentId(3, 3),
                    new LSMDiskComponentId(2, 2), new LSMDiskComponentId(1, 1)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(4, 4), new LSMDiskComponentId(3, 3),
                    new LSMDiskComponentId(2, 2), new LSMDiskComponentId(1, 1)), resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testIDIntervals() {
        try {
            List<ILSMDiskComponentId> componentIDs = Arrays.asList(new LSMDiskComponentId(40, 50),
                    new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29), new LSMDiskComponentId(20, 24),
                    new LSMDiskComponentId(10, 19));

            List<ILSMDiskComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMDiskComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24), new LSMDiskComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24), new LSMDiskComponentId(10, 19)), resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSecondaryMissing() {
        try {
            List<ILSMDiskComponentId> primaryComponentIDs = Arrays.asList(new LSMDiskComponentId(40, 50),
                    new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29), new LSMDiskComponentId(20, 24),
                    new LSMDiskComponentId(10, 19));
            List<ILSMDiskComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, primaryComponentIDs, resultPrimaryIDs, 0);

            List<ILSMDiskComponentId> secondaryComponentIDs = Arrays.asList(new LSMDiskComponentId(30, 35),
                    new LSMDiskComponentId(25, 29), new LSMDiskComponentId(20, 24));
            List<ILSMDiskComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, secondaryComponentIDs, resultSecondaryIDs, 0);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary);

            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);
            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24), new LSMDiskComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24)), resultSecondaryIDs);

        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMultiPartition() {
        try {
            List<ILSMDiskComponentId> componentIDs = Arrays.asList(new LSMDiskComponentId(40, 50),
                    new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29), new LSMDiskComponentId(20, 24),
                    new LSMDiskComponentId(10, 19));

            List<ILSMDiskComponentId> resultPrimaryIDs = new ArrayList<>();
            IndexInfo primary = mockIndex(true, componentIDs, resultPrimaryIDs, 0);

            List<ILSMDiskComponentId> resultSecondaryIDs = new ArrayList<>();
            IndexInfo secondary = mockIndex(false, componentIDs, resultSecondaryIDs, 0);

            List<ILSMDiskComponentId> resultSecondaryIDs1 = new ArrayList<>();
            IndexInfo secondary1 = mockIndex(false, componentIDs, resultSecondaryIDs, 1);

            ILSMMergePolicy policy = mockMergePolicy(primary, secondary, secondary1);
            policy.diskComponentAdded(secondary.getIndex(), false);
            Assert.assertTrue(resultPrimaryIDs.isEmpty());
            Assert.assertTrue(resultSecondaryIDs.isEmpty());

            policy.diskComponentAdded(primary.getIndex(), false);

            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24), new LSMDiskComponentId(10, 19)), resultPrimaryIDs);
            Assert.assertEquals(Arrays.asList(new LSMDiskComponentId(30, 35), new LSMDiskComponentId(25, 29),
                    new LSMDiskComponentId(20, 24), new LSMDiskComponentId(10, 19)), resultSecondaryIDs);
            Assert.assertTrue(resultSecondaryIDs1.isEmpty());
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    private ILSMMergePolicy mockMergePolicy(IndexInfo... indexes) {
        Map<String, String> properties = new HashMap<>();
        properties.put("max-tolerance-component-count", String.valueOf(MAX_COMPONENT_COUNT));
        properties.put("max-mergable-component-size", String.valueOf(MAX_COMPONENT_SIZE));

        Set<IndexInfo> indexInfos = new HashSet<>();
        for (IndexInfo info : indexes) {
            indexInfos.add(info);
        }

        DatasetInfo dsInfo = Mockito.mock(DatasetInfo.class);
        Mockito.when(dsInfo.getDatsetIndexInfos()).thenReturn(indexInfos);

        IDatasetLifecycleManager manager = Mockito.mock(IDatasetLifecycleManager.class);
        Mockito.when(manager.getDatasetInfo(DATASET_ID)).thenReturn(dsInfo);

        ILSMMergePolicy policy = new CorrelatedPrefixMergePolicy(manager, DATASET_ID);
        policy.configure(properties);
        return policy;
    }

    private IndexInfo mockIndex(boolean isPrimary, List<ILSMDiskComponentId> componentIDs,
            List<ILSMDiskComponentId> resultComponentIDs, int partition) throws HyracksDataException {
        List<ILSMDiskComponent> components = new ArrayList<>();
        for (ILSMDiskComponentId id : componentIDs) {
            ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
            Mockito.when(component.getComponentId()).thenReturn(id);
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
                List<ILSMDiskComponent> mergedComponents = invocation.getArgumentAt(1, List.class);
                mergedComponents.forEach(component -> {
                    try {
                        resultComponentIDs.add(component.getComponentId());
                    } catch (HyracksDataException e) {
                        e.printStackTrace();
                    }
                });
                return null;
            }
        }).when(accessor).scheduleMerge(Mockito.any(ILSMIOOperationCallback.class),
                Mockito.anyListOf(ILSMDiskComponent.class));

        Mockito.when(index.createAccessor(Mockito.any(IModificationOperationCallback.class),
                Mockito.any(ISearchOperationCallback.class))).thenReturn(accessor);
        Mockito.when(index.isPrimaryIndex()).thenReturn(isPrimary);

        return new IndexInfo(index, DATASET_ID, 0, partition);
    }

}
