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

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResourceStorageStats {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final DatasetResourceReference ref;
    private final Map<String, Long> components;
    private final long totalSize;

    public ResourceStorageStats(DatasetResourceReference ref, Map<String, Long> components, long totalSize) {
        this.ref = ref;
        this.components = components;
        this.totalSize = totalSize;
    }

    public DatasetResourceReference getRef() {
        return ref;
    }

    public Map<String, Long> getComponents() {
        return components;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public JsonNode asJson() {
        final ObjectNode json = OBJECT_MAPPER.createObjectNode();
        json.put("index", ref.getIndex());
        json.put("dataset", ref.getDataset());
        json.put("datasetId", ref.getDatasetId());
        json.put("partition", ref.getPartitionId());
        json.put("path", ref.getRelativePath().toString());
        json.put("totalSize", totalSize);
        final ArrayNode componentsJson = OBJECT_MAPPER.createArrayNode();
        components.forEach((id, size) -> {
            ObjectNode component = OBJECT_MAPPER.createObjectNode();
            component.put("id", id);
            component.put("size", size);
            componentsJson.add(component);
        });
        json.set("components", componentsJson);
        return json;
    }
}
