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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ICacheMemoryAllocator {
    /**
     * @param pageSize
     * @param numPages
     * @return a ByteBuffer array of size numPages and each buffer of size pageSize.
     */
    public ByteBuffer[] allocate(int pageSize, int numPages);

    /**
     * Ensures the availability of the memory budget with the ResourceMemoryManager (if exists) before allocation. Otherwise, it acts as a call to {@link #allocate(int, int)}
     *
     * @param pageSize
     * @param numPages
     * @return
     * @throws HyracksDataException
     */
    public ByteBuffer[] ensureAvailabilityThenAllocate(int pageSize, int numPages) throws HyracksDataException;

    /**
     * Reserves the allocation with the ResourceMemoryManager (if exists). Otherwise, a HyracksDataException is thrown.
     * Typically the memory will be subsequently allocated with call to allocate()
     *
     * @param pageSize
     * @param numPages
     * @return
     * @throws HyracksDataException
     */
    public void reserveAllocation(int pageSize, int numPages) throws HyracksDataException;

}
