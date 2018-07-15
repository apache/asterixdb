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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

public class Checkpoint implements Comparable<Checkpoint>, IJsonSerializable {

    private static final long serialVersionUID = 1L;
    private final long checkpointLsn;
    private final long minMCTFirstLsn;
    private final long maxTxnId;
    private final long timeStamp;
    private final boolean sharp;
    private final int storageVersion;

    public Checkpoint(long checkpointLsn, long minMCTFirstLsn, long maxTxnId, long timeStamp, boolean sharp,
            int storageVersion) {
        this.checkpointLsn = checkpointLsn;
        this.minMCTFirstLsn = minMCTFirstLsn;
        this.maxTxnId = maxTxnId;
        this.timeStamp = timeStamp;
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

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean isSharp() {
        return sharp;
    }

    public int getStorageVersion() {
        return storageVersion;
    }

    @Override
    public int compareTo(Checkpoint checkpoint) {
        long compareTimeStamp = checkpoint.getTimeStamp();

        // Descending order
        long diff = compareTimeStamp - this.timeStamp;
        if (diff > 0) {
            return 1;
        } else if (diff == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Checkpoint)) {
            return false;
        }
        Checkpoint other = (Checkpoint) obj;
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (checkpointLsn ^ (checkpointLsn >>> 32));
        result = prime * result + Long.hashCode(maxTxnId);
        result = prime * result + (int) (minMCTFirstLsn ^ (minMCTFirstLsn >>> 32));
        result = prime * result + (sharp ? 1231 : 1237);
        result = prime * result + storageVersion;
        result = prime * result + (int) (timeStamp ^ (timeStamp >>> 32));
        return result;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode checkpointJson = registry.getClassIdentifier(getClass(), serialVersionUID);
        checkpointJson.put("checkpointLsn", checkpointLsn);
        checkpointJson.put("minMCTFirstLsn", minMCTFirstLsn);
        checkpointJson.put("maxTxnId", maxTxnId);
        checkpointJson.put("timeStamp", timeStamp);
        checkpointJson.put("sharp", timeStamp);
        checkpointJson.put("storageVersion", storageVersion);
        return checkpointJson;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        long checkpointLsn = json.get("checkpointLsn").asLong();
        long minMCTFirstLsn = json.get("minMCTFirstLsn").asLong();
        long maxTxnId = json.get("maxTxnId").asLong();
        long timeStamp = json.get("timeStamp").asLong();
        boolean sharp = json.get("sharp").asBoolean();
        int storageVersion = json.get("storageVersion").asInt();
        return new Checkpoint(checkpointLsn, minMCTFirstLsn, maxTxnId, timeStamp, sharp, storageVersion);
    }
}