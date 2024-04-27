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
package org.apache.hyracks.storage.am.lsm.common.cloud;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.disk.ISweepContext;

/**
 * Disk cache manager for an index
 */
public interface IIndexDiskCacheManager {
    /**
     * @return the manager is active
     * @see ILSMIndex#activate()
     */
    boolean isActive();

    /**
     * @return whether an index can be swept to make space in {@link IPhysicalDrive}
     */
    boolean isSweepable();

    /**
     * Prepare a sweep plan
     *
     * @return true if the plan determines a sweep can be performed
     */
    boolean prepareSweepPlan();

    /**
     * Sweep an index to make space in {@link IPhysicalDrive}
     *
     * @param context sweep context
     * @return freed space in bytes
     */
    long sweep(ISweepContext context) throws HyracksDataException;
}
