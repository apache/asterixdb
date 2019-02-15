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

import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.context.DatasetMemoryManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DatasetMemoryManagerTest {

    private static final StorageProperties storageProperties;
    private static final long GLOBAL_BUDGET = 1000L;
    private static final long METADATA_DATASET_BUDGET = 200L;
    private static final long DATASET_BUDGET = 400L;

    static {
        storageProperties = Mockito.mock(StorageProperties.class);
        Mockito.when(storageProperties.getMemoryComponentGlobalBudget()).thenReturn(GLOBAL_BUDGET);
        Mockito.when(storageProperties.getMemoryComponentNumPages()).thenReturn(8);
        Mockito.when(storageProperties.getMetadataMemoryComponentNumPages()).thenReturn(4);
        Mockito.when(storageProperties.getMemoryComponentPageSize()).thenReturn(50);
        Mockito.when(storageProperties.getMemoryComponentsNum()).thenReturn(2);
    }

    @Test
    public void allocate() {
        DatasetMemoryManager memoryManager = new DatasetMemoryManager(storageProperties);
        // double allocate
        Assert.assertTrue(memoryManager.allocate(1));
        boolean thrown = false;
        try {
            memoryManager.allocate(1);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("already allocated"));
            thrown = true;
        }
        Assert.assertTrue(thrown);

        // allocate metadata and non-metadata datasets
        Assert.assertTrue(memoryManager.allocate(400));

        long expectedBudget = GLOBAL_BUDGET - METADATA_DATASET_BUDGET - DATASET_BUDGET;
        Assert.assertEquals(memoryManager.getAvailable(), expectedBudget);

        // reserve after allocate shouldn't allocate the budget again
        Assert.assertTrue(memoryManager.allocate(401));
        Assert.assertTrue(memoryManager.reserve(401));

        // deallocate should still keep the reserved memory
        memoryManager.deallocate(401);
        expectedBudget = GLOBAL_BUDGET - METADATA_DATASET_BUDGET - (DATASET_BUDGET * 2);
        Assert.assertEquals(memoryManager.getAvailable(), expectedBudget);

        // exceed budget should return false
        Assert.assertFalse(memoryManager.allocate(402));
    }

    @Test
    public void reserve() {
        DatasetMemoryManager memoryManager = new DatasetMemoryManager(storageProperties);
        // reserve then allocate budget
        Assert.assertTrue(memoryManager.reserve(1));
        Assert.assertTrue(memoryManager.allocate(1));
        long expectedBudget = GLOBAL_BUDGET - METADATA_DATASET_BUDGET;
        Assert.assertEquals(memoryManager.getAvailable(), expectedBudget);

        // double reserve
        Assert.assertTrue(memoryManager.reserve(2));

        // cancel reserved
        memoryManager.cancelReserved(2);
        Assert.assertEquals(memoryManager.getAvailable(), expectedBudget);
    }

    @Test
    public void deallocate() {
        DatasetMemoryManager memoryManager = new DatasetMemoryManager(storageProperties);
        // deallocate reserved
        Assert.assertTrue(memoryManager.reserve(200));
        Assert.assertTrue(memoryManager.allocate(200));
        memoryManager.deallocate(200);
        long expectedBudget = GLOBAL_BUDGET - DATASET_BUDGET;
        Assert.assertEquals(memoryManager.getAvailable(), expectedBudget);

        // deallocate not allocated
        boolean thrown = false;
        try {
            memoryManager.deallocate(1);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("No allocated"));
            thrown = true;
        }
        Assert.assertTrue(thrown);

        // double deallocate
        memoryManager.allocate(2);
        memoryManager.deallocate(2);
        thrown = false;
        try {
            memoryManager.deallocate(2);
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("No allocated"));
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }
}
