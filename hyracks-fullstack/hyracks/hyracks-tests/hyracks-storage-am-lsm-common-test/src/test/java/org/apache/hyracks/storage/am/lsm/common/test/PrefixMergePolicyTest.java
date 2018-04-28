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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicy;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import junit.framework.TestCase;

public class PrefixMergePolicyTest extends TestCase {

    private final int MAX_COMPONENT_SIZE = 100;

    private final int MAX_COMPONENT_COUNT = 3;

    public void testBasic() {
        try {
            List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            assertEquals(3, resultSizes.size());
            assertEquals(Arrays.asList(1L, 2L, 3L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSmallComponents() {
        try {
            List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            assertEquals(5, resultSizes.size());
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSkipComponents() {
        try {
            List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 101L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            assertEquals(4, resultSizes.size());
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSkipLargeComponents() {
        try {
            List<Long> sizes = new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L, 50L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            assertEquals(4, resultSizes.size());
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMergeLargeComponents() {
        try {
            List<Long> sizes = new ArrayList<>(Arrays.asList(3L, 3L, 45L, 50L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            assertEquals(4, resultSizes.size());
            assertEquals(Arrays.asList(3L, 3L, 45L, 50L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAvoidMergeLargeComponents() {
        try {
            // in this case, we can
            List<Long> sizes = new ArrayList<>(Arrays.asList(28L, 29L, 30L, 90L));
            List<Long> resultSizes = new ArrayList<>();
            ILSMIndex index = mockIndex(sizes, resultSizes);
            ILSMMergePolicy policy = mockMergePolicy();
            policy.diskComponentAdded(index, false);
            //assertEquals(3, resultSizes.size());
            assertEquals(Arrays.asList(28L, 29L, 30L, 90L), resultSizes);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPerformance() {
        try {
            List<Long> sizes = new ArrayList<>();
            ILSMMergePolicy policy = mockMergePolicy();
            int maxNumComponents = 0;
            int sumComponents = 0;
            Set<Long> writeAmplifications = new HashSet<>();
            int pass = 0;
            do {
                pass++;
                sizes.add(0, 1L);
                ILSMIndex index = mockIndex(sizes, null);
                policy.diskComponentAdded(index, false);
                int length = sizes.size();
                maxNumComponents = maxNumComponents >= length ? maxNumComponents : length;
                sumComponents += length;
                writeAmplifications.add(sizes.get(sizes.size() - 1));
            } while (sizes.get(sizes.size() - 1) < MAX_COMPONENT_SIZE);
            writeAmplifications.remove(1L);
            Assert.assertTrue(maxNumComponents <= 6);

            //average disk components per pass
            Assert.assertTrue(sumComponents / pass <= 3);

            Assert.assertTrue(writeAmplifications.size() <= 7);
        } catch (HyracksDataException e) {
            Assert.fail(e.getMessage());
        }
    }

    private ILSMMergePolicy mockMergePolicy() {
        Map<String, String> properties = new HashMap<>();
        properties.put("max-mergable-component-size", String.valueOf(MAX_COMPONENT_SIZE));
        properties.put("max-tolerance-component-count", String.valueOf(MAX_COMPONENT_COUNT));
        ILSMMergePolicy policy = new PrefixMergePolicy();
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
                if (mergedSizes != null) {
                    mergedComponents.forEach(component -> {
                        mergedSizes.add(component.getComponentSize());
                    });
                }
                long sum = 0;
                for (ILSMDiskComponent c : mergedComponents) {
                    sum += c.getComponentSize();
                }
                ILSMDiskComponent component = Mockito.mock(ILSMDiskComponent.class);
                Mockito.when(component.getComponentSize()).thenReturn(sum);
                Mockito.when(component.getState()).thenReturn(ComponentState.READABLE_UNWRITABLE);
                int swapIndex = components.indexOf(mergedComponents.get(0));
                components.removeAll(mergedComponents);
                components.add(swapIndex, component);
                componentSizes.clear();
                for (ILSMDiskComponent c : components) {
                    componentSizes.add(c.getComponentSize());
                }
                return null;
            }
        }).when(accessor).scheduleMerge(Mockito.anyListOf(ILSMDiskComponent.class));

        Mockito.when(index.createAccessor(Mockito.any(IIndexAccessParameters.class))).thenReturn(accessor);

        return index;
    }

}
