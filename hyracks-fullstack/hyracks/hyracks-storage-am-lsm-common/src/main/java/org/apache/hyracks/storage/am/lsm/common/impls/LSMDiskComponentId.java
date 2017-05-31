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

import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentId;

public class LSMDiskComponentId implements ILSMDiskComponentId {

    private final long minId;

    private final long maxId;

    public LSMDiskComponentId(long minId, long maxId) {
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
        if (!(obj instanceof LSMDiskComponentId)) {
            return false;
        }
        LSMDiskComponentId other = (LSMDiskComponentId) obj;
        if (maxId != other.maxId) {
            return false;
        }
        if (minId != other.minId) {
            return false;
        }
        return true;
    }

}
