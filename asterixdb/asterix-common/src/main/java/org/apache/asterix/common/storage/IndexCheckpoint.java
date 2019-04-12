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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexCheckpoint {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long INITIAL_CHECKPOINT_ID = 0;
    // TODO(mblow): remove this marker & related logic once we no longer are able to read indexes prior to the fix
    private static final long HAS_NULL_MISSING_VALUES_FIX = -1;
    private long id;
    private long validComponentSequence;
    private long lowWatermark;
    private long lastComponentId;
    private Map<Long, Long> masterNodeFlushMap;

    public static IndexCheckpoint first(long lastComponentSequence, long lowWatermark, long validComponentId) {
        IndexCheckpoint firstCheckpoint = new IndexCheckpoint();
        firstCheckpoint.id = INITIAL_CHECKPOINT_ID;
        firstCheckpoint.lowWatermark = lowWatermark;
        firstCheckpoint.validComponentSequence = lastComponentSequence;
        firstCheckpoint.lastComponentId = validComponentId;
        firstCheckpoint.masterNodeFlushMap = new HashMap<>();
        firstCheckpoint.masterNodeFlushMap.put(HAS_NULL_MISSING_VALUES_FIX, HAS_NULL_MISSING_VALUES_FIX);
        return firstCheckpoint;
    }

    public static IndexCheckpoint next(IndexCheckpoint latest, long lowWatermark, long validComponentSequence,
            long lastComponentId) {
        if (lowWatermark < latest.getLowWatermark()) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("low watermark {} less than the latest checkpoint low watermark {}", lowWatermark, latest);
            }
            throw new IllegalStateException("Low watermark should always be increasing");
        }
        IndexCheckpoint next = new IndexCheckpoint();
        next.id = latest.getId() + 1;
        next.lowWatermark = lowWatermark;
        next.lastComponentId = lastComponentId;
        next.validComponentSequence = validComponentSequence;
        next.masterNodeFlushMap = latest.getMasterNodeFlushMap();
        // remove any lsn from the map that wont be used anymore
        next.masterNodeFlushMap.values().removeIf(lsn -> lsn <= lowWatermark && lsn != HAS_NULL_MISSING_VALUES_FIX);
        return next;
    }

    @JsonCreator
    private IndexCheckpoint() {
    }

    public long getValidComponentSequence() {
        return validComponentSequence;
    }

    public long getLowWatermark() {
        return lowWatermark;
    }

    public long getLastComponentId() {
        return lastComponentId;
    }

    public Map<Long, Long> getMasterNodeFlushMap() {
        return masterNodeFlushMap;
    }

    public long getId() {
        return id;
    }

    public boolean hasNullMissingValuesFix() {
        return masterNodeFlushMap.containsKey(HAS_NULL_MISSING_VALUES_FIX);
    }

    public String asJson() throws HyracksDataException {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static IndexCheckpoint fromJson(String json) throws HyracksDataException {
        try {
            return OBJECT_MAPPER.readValue(json, IndexCheckpoint.class);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        try {
            return asJson();
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }
}
