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

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Checkpoint implements Comparable<Checkpoint> {

    private final long checkpointLsn;
    private final long minMCTFirstLsn;
    private final int maxJobId;
    private final long timeStamp;
    private final boolean sharp;
    private final int storageVersion;

    @JsonCreator
    public Checkpoint(@JsonProperty("checkpointLsn") long checkpointLsn,
            @JsonProperty("minMCTFirstLsn") long minMCTFirstLsn, @JsonProperty("maxJobId") int maxJobId,
            @JsonProperty("timeStamp") long timeStamp, @JsonProperty("sharp") boolean sharp,
            @JsonProperty("storageVersion") int storageVersion) {
        this.checkpointLsn = checkpointLsn;
        this.minMCTFirstLsn = minMCTFirstLsn;
        this.maxJobId = maxJobId;
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

    public int getMaxJobId() {
        return maxJobId;
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
        result = prime * result + maxJobId;
        result = prime * result + (int) (minMCTFirstLsn ^ (minMCTFirstLsn >>> 32));
        result = prime * result + (sharp ? 1231 : 1237);
        result = prime * result + storageVersion;
        result = prime * result + (int) (timeStamp ^ (timeStamp >>> 32));
        return result;
    }

    public String asJson() throws HyracksDataException {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static Checkpoint fromJson(String json) throws HyracksDataException {
        try {
            return new ObjectMapper().readValue(json, Checkpoint.class);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}