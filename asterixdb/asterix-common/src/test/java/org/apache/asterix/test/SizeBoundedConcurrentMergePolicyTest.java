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

package org.apache.asterix.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.storage.SizeBoundedConcurrentMergePolicy;
import org.apache.asterix.common.storage.SizeBoundedConcurrentMergePolicyFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SizeBoundedConcurrentMergePolicyTest {

    private static final int MIN_MERGE_COMPONENT_COUNT = 3;
    private static final int MAX_MERGE_COMPONENT_COUNT = 5;
    private static final int MAX_COMPONENT_COUNT = 10;
    private static final long MAX_STORAGE_COMPONENT_SIZE = 30L;
    private static final double SIZE_RATIO = 1.0;

    @Test
    public void testBasic() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(List.of(1L, 2L, 3L, 6L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes, resultSizes);
    }

    @Test
    public void testNotEnoughComponents() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertTrue(resultSizes.isEmpty());
    }

    @Test
    public void testNoFullMergeAllowed() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();

        IllegalArgumentException fullMergeException = Assert.assertThrows(IllegalArgumentException.class, () -> {
            policy.diskComponentAdded(index, true);
        });

        Assert.assertEquals("SizeBoundedConcurrentMergePolicy does not support fullMerge.",
                fullMergeException.getMessage());
    }

    // [ 1, 2, 3] [7] -> this is not a valid range, as (1 + 2 + 3) * 1(size-ratio) >= 7
    // [ 1, 2] [3] -> A valid range, as 1+2+3 < 30 (max component count) && (1+2) * 1 >= 3
    @Test
    public void testSkipLargeComponent() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 7L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes.subList(0, 3), resultSizes);
    }

    @Test
    public void testSkipMergeComponent0_2() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 17L, 20L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertTrue(resultSizes.isEmpty());
    }

    @Test
    public void testAvoidExceedingMaxSize() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(3L, 9L, 11L, 20L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertEquals(sizes.subList(0, 3), resultSizes);
    }

    @Test
    public void testNoMergableComponents() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(15L, 16L, 15L, 16L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        policy.diskComponentAdded(index, false);

        Assert.assertTrue(resultSizes.isEmpty());
    }

    @Test
    public void testConcurrentMerge() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 100L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        // component 5L is merging
        Mockito.when(index.getDiskComponents().get(4).getState())
                .thenReturn(ILSMComponent.ComponentState.READABLE_MERGING);

        policy.diskComponentAdded(index, false);
        Assert.assertEquals(sizes.subList(0, 4), resultSizes);
    }

    @Test
    public void testMergeLagging() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(
                Arrays.asList(1L, 2L, 3L, 4L, 29L, 1L, 2L, 3L, 4L, 29L, 1L, 2L, 3L, 4L, 29L, 1L, 2L, 3L, 4L, 29L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        Assert.assertTrue(policy.isMergeLagging(index));
        Assert.assertEquals(sizes.subList(15, 19), resultSizes);
    }

    @Test
    public void testNoMergeLagging() throws HyracksDataException {
        List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 29L, 1L, 2L, 3L, 4L));
        List<Long> resultSizes = new ArrayList<>();
        ILSMIndex index = mockIndex(sizes, resultSizes);
        ILSMMergePolicy policy = createMergePolicy();
        Assert.assertFalse(policy.isMergeLagging(index));
    }

    private ILSMMergePolicy createMergePolicy() {
        Map<String, String> properties = new HashMap<>();
        properties.put(SizeBoundedConcurrentMergePolicyFactory.MIN_MERGE_COMPONENT_COUNT,
                String.valueOf(MIN_MERGE_COMPONENT_COUNT));
        properties.put(SizeBoundedConcurrentMergePolicyFactory.MAX_MERGE_COMPONENT_COUNT,
                String.valueOf(MAX_MERGE_COMPONENT_COUNT));
        properties.put(SizeBoundedConcurrentMergePolicyFactory.SIZE_RATIO, String.valueOf(SIZE_RATIO));
        properties.put(SizeBoundedConcurrentMergePolicyFactory.MAX_COMPONENT_COUNT,
                String.valueOf(MAX_COMPONENT_COUNT));

        ILSMMergePolicy policy = new SizeBoundedConcurrentMergePolicy(MAX_STORAGE_COMPONENT_SIZE);
        policy.configure(properties);
        return policy;
    }

    private ILSMIndex mockIndex(List<Long> componentSizes, List<Long> mergedSizes) throws HyracksDataException {
        List<ILSMDiskComponent> components = new ArrayList<>();
        for (Long size : componentSizes) {
            ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
            Mockito.when(component.getComponentSize()).thenReturn(size);
            Mockito.when(component.getState()).thenReturn(ILSMComponent.ComponentState.READABLE_UNWRITABLE);
            components.add(component);
        }

        ILSMIndex index = Mockito.mock(ILSMIndex.class);
        Mockito.when(index.getDiskComponents()).thenReturn(components);

        ILSMIndexAccessor accessor = Mockito.mock(ILSMIndexAccessor.class);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                List<ILSMDiskComponent> mergedComponents = invocation.getArgument(0);
                mergedComponents.forEach(component -> {
                    mergedSizes.add(component.getComponentSize());
                });
                return null;
            }
        }).when(accessor).scheduleMerge(Mockito.anyListOf(ILSMDiskComponent.class));

        Mockito.when(index.createAccessor(Mockito.any(IIndexAccessParameters.class))).thenReturn(accessor);

        return index;
    }
}
