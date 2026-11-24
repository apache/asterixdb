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
package org.apache.hyracks.cloud.cache.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.cloud.IIndexDiskCacheManager;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.junit.Assert;
import org.junit.Test;

/**
 * The sweeper evicts the files at the path of every inactive resource, that is, every local resource that has not
 * been registered since the node listed them, with no further check. So no path may stay listed once an index
 * registers there.
 */
public class CloudDiskResourceCacheLockNotifierTest {
    private static final String ORDERS = "storage/partition_0/rc/Orders/0/Orders";
    private static final String CUSTOMERS = "storage/partition_0/rc/Customers/0/Customers";

    @Test
    public void registeringAnIndexClearsItsEntry() {
        CloudDiskResourceCacheLockNotifier notifier = listed(resource(101, ORDERS), resource(102, CUSTOMERS));
        notifier.onRegister(resource(101, ORDERS), unsweepableIndex());
        Assert.assertEquals(List.of(CUSTOMERS), inactivePaths(notifier));
    }

    @Test
    public void anIndexCreatedAtTheDroppedIndexPathClearsItsEntry() {
        CloudDiskResourceCacheLockNotifier notifier = listed(resource(101, ORDERS), resource(102, CUSTOMERS));
        // index 101 is dropped before it is ever opened, which the notifier is not told of, and the index created
        // with the same name takes its path under a new id
        notifier.onRegister(resource(205, ORDERS), unsweepableIndex());
        Assert.assertEquals(List.of(CUSTOMERS), inactivePaths(notifier));
    }

    private static CloudDiskResourceCacheLockNotifier listed(LocalResource... resources) {
        CloudDiskResourceCacheLockNotifier notifier = new CloudDiskResourceCacheLockNotifier(lr -> true);
        notifier.reportLocalResources(Arrays.stream(resources).collect(Collectors.toMap(LocalResource::getId, r -> r)));
        return notifier;
    }

    private static List<String> inactivePaths(CloudDiskResourceCacheLockNotifier notifier) {
        return notifier.getInactiveResources().stream().map(LocalResource::getPath).sorted()
                .collect(Collectors.toList());
    }

    private static LocalResource resource(long id, String path) {
        IResource resource = mock(IResource.class);
        when(resource.getPath()).thenReturn(path);
        return new LocalResource(id, 0, true, resource);
    }

    private static ILSMIndex unsweepableIndex() {
        IIndexDiskCacheManager diskCacheManager = mock(IIndexDiskCacheManager.class);
        when(diskCacheManager.isSweepable()).thenReturn(false);
        ILSMIndex index = mock(ILSMIndex.class);
        when(index.getDiskCacheManager()).thenReturn(diskCacheManager);
        return index;
    }
}
