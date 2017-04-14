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

package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ILSMMergePolicy {
    void diskComponentAdded(ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException;

    void configure(Map<String, String> properties);

    /**
     * This method is used for flush-operation flow control:
     * When the space occupancy of the in-memory component exceeds a specified memory threshold,
     * entries are flushed to disk. As entries accumulate on disk, the entries are periodically
     * merged together subject to a merge policy that decides when and what to merge. A merge
     * policy may impose a certain constraint such as a maximum number of mergable(merge-able)
     * disk components in order to provide a reasonable query response time. Otherwise, the query
     * response time gets slower as the number of disk components increases.
     * In order to avoid such an unexpected situation according to the merge policy, a way to
     * control the number of disk components is provided by introducing a new method,
     * isMegeLagging() in ILSMMergePolicy interface. When flushing an in-memory component is completed,
     * the provided isMergeLagging() method is called to decide whether the memory budget for the
     * current flushed in-memory component should be available for the incoming updated(inserted/deleted/updated)
     * entries or not. If the method returns true, i.e., the merge operation is lagged according to
     * the merge policy, the memory budget will not be made available for the incoming entries by
     * making the current flush operation thread wait until (ongoing) merge operation finishes.
     * Therefore, this will effectively prevent the number of disk components from exceeding
     * a threshold of the allowed number of disk components.
     *
     * @param index
     * @return true if merge operation is lagged according to the implemented merge policy,
     *         false otherwise.
     * @throws HyracksDataException
     * @throws IndexException
     */
    boolean isMergeLagging(ILSMIndex index) throws HyracksDataException;
}
