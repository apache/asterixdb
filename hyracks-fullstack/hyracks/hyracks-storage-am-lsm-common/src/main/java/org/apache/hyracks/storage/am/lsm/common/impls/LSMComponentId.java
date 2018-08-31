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

package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;

public class LSMComponentId implements ILSMComponentId {

    public static final long NOT_FOUND = -1;
    public static final long MIN_VALID_COMPONENT_ID = 0;

    // Used to represent an empty index with no components
    public static final LSMComponentId EMPTY_INDEX_LAST_COMPONENT_ID = new LSMComponentId(NOT_FOUND, NOT_FOUND);

    // A default component id used for bulk loaded component
    public static final LSMComponentId DEFAULT_COMPONENT_ID =
            new LSMComponentId(MIN_VALID_COMPONENT_ID, MIN_VALID_COMPONENT_ID);

    private long minId;

    private long maxId;

    public LSMComponentId(long minId, long maxId) {
        assert minId <= maxId;
        this.minId = minId;
        this.maxId = maxId;
    }

    public void reset(long minId, long maxId) {
        this.minId = minId;
        this.maxId = maxId;
    }

    @Override
    public long getMinId() {
        return this.minId;
    }

    @Override
    public long getMaxId() {
        return this.maxId;
    }

    @Override
    public boolean missing() {
        return minId == NOT_FOUND || maxId == NOT_FOUND;
    }

    @Override
    public String toString() {
        return "[" + minId + "," + maxId + "]";
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(minId) + Long.hashCode(maxId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LSMComponentId)) {
            return false;
        }
        LSMComponentId other = (LSMComponentId) obj;
        return maxId == other.maxId && minId == other.minId;
    }

    @Override
    public IdCompareResult compareTo(ILSMComponentId id) {
        if (this.missing() || id == null || id.missing()) {
            return IdCompareResult.UNKNOWN;
        }
        LSMComponentId componentId = (LSMComponentId) id;
        if (this.getMinId() > componentId.getMaxId()) {
            return IdCompareResult.GREATER_THAN;
        } else if (this.getMaxId() < componentId.getMinId()) {
            return IdCompareResult.LESS_THAN;
        } else if (this.getMinId() <= componentId.getMinId() && this.getMaxId() >= componentId.getMaxId()) {
            return IdCompareResult.INCLUDE;
        } else {
            return IdCompareResult.INTERSECT;
        }
    }

}
