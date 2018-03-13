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
package org.apache.asterix.api.http.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NodeControllerDetailsHelper {
    private NodeControllerDetailsHelper() {
    }

    public static ObjectNode fixupKeys(ObjectNode json) {
        // TODO (mblow): generate the keys with _ to begin with
        List<String> keys = new ArrayList<>();
        for (Iterator<String> iter = json.fieldNames(); iter.hasNext();) {
            keys.add(iter.next());
        }
        for (String key : keys) {
            String newKey = key.replace('-', '_');
            if (!newKey.equals(key)) {
                json.set(newKey, json.remove(key));
            }
        }
        return json;
    }

    public static ObjectNode processNodeDetailsJSON(ObjectNode json, ObjectMapper om) {
        int index = json.get("rrd-ptr").asInt() - 1;
        json.remove("rrd-ptr");

        List<String> keys = new ArrayList<>();
        for (Iterator<String> iter = json.fieldNames(); iter.hasNext();) {
            keys.add(iter.next());
        }
        final ArrayNode gcNames = (ArrayNode) json.get("gc-names");
        final ArrayNode gcCollectionTimes = (ArrayNode) json.get("gc-collection-times");
        final ArrayNode gcCollectionCounts = (ArrayNode) json.get("gc-collection-counts");

        for (String key : keys) {
            if (key.startsWith("gc-")) {
                json.remove(key);
            } else {
                final JsonNode keyNode = json.get(key);
                if (keyNode instanceof ArrayNode) {
                    final ArrayNode valueArray = (ArrayNode) keyNode;
                    // fixup an index of -1 to the final element in the array (i.e. RRD_SIZE)
                    if (index == -1) {
                        index = valueArray.size() - 1;
                    }
                    final JsonNode value = valueArray.get(index);
                    json.remove(key);
                    json.set(key.replaceAll("s$", ""), value);
                }
            }
        }
        ArrayNode gcs = om.createArrayNode();

        for (int i = 0; i < gcNames.size(); i++) {
            ObjectNode gc = om.createObjectNode();
            gc.set("name", gcNames.get(i));
            gc.set("collection-time", gcCollectionTimes.get(i).get(index));
            gc.set("collection-count", gcCollectionCounts.get(i).get(index));
            fixupKeys(gc);
            gcs.add(gc);
        }
        json.set("gcs", gcs);

        return json;
    }
}
