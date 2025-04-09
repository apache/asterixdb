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
package org.apache.asterix.common.storage;

public class StorageIOStats {

    private int pendingFlushes;
    private int pendingMerges;
    private int pendingReplications;
    private int failedFlushes;
    private int failedMerges;

    public void addPendingFlushes(int pending) {
        pendingFlushes += pending;
    }

    public void addPendingMerges(int pending) {
        pendingMerges += pending;
    }

    public void addPendingReplications(int pending) {
        pendingReplications += pending;
    }

    public void addFailedFlushes(int failed) {
        failedFlushes += failed;
    }

    public void addFailedMerges(int failed) {
        failedMerges += failed;
    }

    public int getFailedFlushes() {
        return failedFlushes;
    }

    public int getFailedMerges() {
        return failedMerges;
    }

    public int getPendingFlushes() {
        return pendingFlushes;
    }

    public int getPendingMerges() {
        return pendingMerges;
    }

    public int getPendingReplications() {
        return pendingReplications;
    }

    public int getTotalPendingOperations() {
        return pendingFlushes + pendingMerges + pendingReplications;
    }
}
