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
package org.apache.asterix.transaction.management.service.locking;

/**
 * A hash table for ResourceGroups. As each ResourceGroup has a latch that protects the modifications for resources in
 * that group, the size of a ResourceGroupTable determines the maximal number of lock requests that can concurrently
 * be served by a ConcurrentLockManager.
 *
 * @see ResourceGroup
 * @see ConcurrentLockManager
 */

class ResourceGroupTable {
    public final int size;

    private ResourceGroup[] table;

    public ResourceGroupTable(int size) {
        this.size = size;
        table = new ResourceGroup[size];
        for (int i = 0; i < size; ++i) {
            table[i] = new ResourceGroup();
        }
    }

    ResourceGroup get(int dId, int entityHashValue) {
        // TODO ensure good properties of hash function
        int h = Math.abs(dId ^ entityHashValue);
        if (h < 0)
            h = 0;
        return table[h % size];
    }

    ResourceGroup get(int i) {
        return table[i];
    }

    public void getAllLatches() {
        for (int i = 0; i < size; ++i) {
            table[i].getLatch();
        }
    }

    public void releaseAllLatches() {
        for (int i = 0; i < size; ++i) {
            table[i].releaseLatch();
        }
    }

    public StringBuilder append(StringBuilder sb) {
        return append(sb, false);
    }

    public StringBuilder append(StringBuilder sb, boolean detail) {
        for (int i = 0; i < table.length; ++i) {
            sb.append(i).append(" : ");
            if (detail) {
                sb.append(table[i]);
            } else {
                sb.append(table[i].firstResourceIndex);
            }
            sb.append('\n');
        }
        return sb;
    }
}
