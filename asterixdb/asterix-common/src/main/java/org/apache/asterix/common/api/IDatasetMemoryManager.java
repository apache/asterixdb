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
package org.apache.asterix.common.api;

public interface IDatasetMemoryManager {

    /**
     * Allocates memory for dataset {@code datasetId}.
     *
     * @param datasetId
     * @return true, if the allocation is successful, otherwise false.
     */
    boolean allocate(int datasetId);

    /**
     * Deallocates memory of dataset {@code datasetId}.
     *
     * @param datasetId
     */
    void deallocate(int datasetId);

    /**
     * Reserves memory for dataset {@code datasetId}. The reserved memory
     * is guaranteed to be allocatable when needed for the dataset. Reserve
     * maybe called after allocation to reserve the allocated budget
     * on deallocation.
     *
     * @param datasetId
     * @return true, if the allocation is successful, otherwise false.
     */
    boolean reserve(int datasetId);

    /**
     * Cancels the reserved memory for dataset {@code datasetId}.
     *
     * @param datasetId
     */
    void cancelReserved(int datasetId);

    /**
     * @return The remaining memory budget that can be used for datasets.
     */
    long getAvailable();

    /**
     * @param datasetId
     * @return The number of virtual buffer cache pages that should be allocated for dataset {@code datasetId}.
     */
    int getNumPages(int datasetId);
}
