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

package org.apache.hyracks.storage.am.lsm.common.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicyFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ConcurrentMergePolicyTest {

    private static final int MIN_MERGE_COMPONENT_COUNT = 3;
    private static final int MAX_MERGE_COMPONENT_COUNT = 5;
    private static final int MAX_COMPONENT_COUNT = 10;
    private static final double SIZE_RATIO = 1.0;

    @Test
    public void testBasic() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 6L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes, resultSizes);
    }

    @Test
    public void testNotEnoughComponents() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertTrue(resultSizes.isEmpty());
    }

    @Test
    public void testSkipLargeComponent() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 7L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes.subList(0, 3), resultSizes);
    }

    @Test
    public void testMaxMergeComponentCount() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 100L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes.subList(2, 2 + MAX_MERGE_COMPONENT_COUNT), resultSizes);
    }

    @Test
    public void testConcurrentMerge() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 100L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        // component 5L is merging
        Mockito.when(index.getDiskComponents().get(4).getState()).thenReturn(ComponentState.READABLE_MERGING);

        policy.diskComponentAdded(index, false);
        Assert.assertEquals(sizes.subList(0, 4), resultSizes);
    }

    @Test
    public void testNoMergeLagging() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 100L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        Assert.assertFalse(policy.isMergeLagging(index));
    }

    @Test
    public void testMergeLagging() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        Assert.assertTrue(policy.isMergeLagging(index));
        // there should be a merge scheduled
        Assert.assertEquals(sizes.subList(6, 11), resultSizes);
    }

    @Test
    public void testMergeLaggingFullMerge() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L, 1024L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createkMergePolicy();
        Assert.assertTrue(policy.isMergeLagging(index));
        // there should be a merge scheduled
        Assert.assertEquals(sizes, resultSizes);
    }

    private ILSMMergePolicy createkMergePolicy() {
        Map<String, String> properties = new HashMap<>();
        properties.put(ConcurrentMergePolicyFactory.MAX_COMPONENT_COUNT, String.valueOf(MAX_COMPONENT_COUNT));
        properties.put(ConcurrentMergePolicyFactory.SIZE_RATIO, String.valueOf(SIZE_RATIO));
        properties.put(ConcurrentMergePolicyFactory.MIN_MERGE_COMPONENT_COUNT,
                String.valueOf(MIN_MERGE_COMPONENT_COUNT));
        properties.put(ConcurrentMergePolicyFactory.MAX_MERGE_COMPONENT_COUNT,
                String.valueOf(MAX_MERGE_COMPONENT_COUNT));

        ILSMMergePolicy policy = new ConcurrentMergePolicy();
        policy.configure(properties);
        return policy;
    }

    private ILSMIndex mockIndex(List<Long> componentSizes, List<Long> mergedSizes) throws HyracksDataException {
        List<ILSMDiskComponent> components = new ArrayList<>();
        for (Long size : componentSizes) {
            ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
            Mockito.when(component.getComponentSize()).thenReturn(size);
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
                    mergedSizes.add(component.getComponentSize());
                });
                return null;
            }
        }).when(accessor).scheduleMerge(Mockito.anyListOf(ILSMDiskComponent.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                mergedSizes.addAll(componentSizes);
                return null;
            }
        }).when(accessor).scheduleFullMerge();

        Mockito.when(index.createAccessor(Mockito.any(IIndexAccessParameters.class))).thenReturn(accessor);

        return index;
    }

}
