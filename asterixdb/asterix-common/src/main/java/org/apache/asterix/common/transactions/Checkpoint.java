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
package org.apache.asterix.common.transactions;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Checkpoint implements Comparable<Checkpoint>, IJsonSerializable {

    private static final long serialVersionUID = 1L;
    private final long checkpointLsn;
    private final long minMCTFirstLsn;
    private final long maxTxnId;
    private final boolean sharp;
    private final int storageVersion;
    private long id;

    public Checkpoint(long id, long checkpointLsn, long minMCTFirstLsn, long maxTxnId, boolean sharp,
            int storageVersion) {
        this.id = id;
        this.checkpointLsn = checkpointLsn;
        this.minMCTFirstLsn = minMCTFirstLsn;
        this.maxTxnId = maxTxnId;
        this.sharp = sharp;
        this.storageVersion = storageVersion;
    }

    public long getCheckpointLsn() {
        return checkpointLsn;
    }

    public long getMinMCTFirstLsn() {
        return minMCTFirstLsn;
    }

    public long getMaxTxnId() {
        return maxTxnId;
    }

    public long getId() {
        return id;
    }

    public boolean isSharp() {
        return sharp;
    }

    public int getStorageVersion() {
        return storageVersion;
    }

    @Override
    public int compareTo(Checkpoint other) {
        return Long.compare(this.id, other.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Checkpoint that = (Checkpoint) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode checkpointJson = registry.getClassIdentifier(getClass(), serialVersionUID);
        checkpointJson.put("id", id);
        checkpointJson.put("checkpointLsn", checkpointLsn);
        checkpointJson.put("minMCTFirstLsn", minMCTFirstLsn);
        checkpointJson.put("maxTxnId", maxTxnId);
        checkpointJson.put("sharp", sharp);
        checkpointJson.put("storageVersion", storageVersion);
        return checkpointJson;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        long id = json.get("id").asLong();
        long checkpointLsn = json.get("checkpointLsn").asLong();
        long minMCTFirstLsn = json.get("minMCTFirstLsn").asLong();
        long maxTxnId = json.get("maxTxnId").asLong();
        boolean sharp = json.get("sharp").asBoolean();
        int storageVersion = json.get("storageVersion").asInt();
        return new Checkpoint(id, checkpointLsn, minMCTFirstLsn, maxTxnId, sharp, storageVersion);
    }
}
