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
package org.apache.asterix.column.test.explosion;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A key-range search reads each record whole and as last written, wherever it starts: upserts, deletes, flushes and
 * merges leave components whose versions of a key hide one another, and the search skips the hidden ones.
 */
public class RangeSearchAlignmentTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void aRangeSearchReadsWhatAScanReads() throws Exception {
        long seed = Long.getLong("range.seed", 7L);
        Random random = new Random(seed);
        TreeMap<Integer, JsonNode> expected = new TreeMap<>();
        try (ColumnLsmHarness harness = new ColumnLsmHarness()) {
            int next = 1;
            for (int round = 0; round < 8; round++) {
                List<String> batch = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    int id = i < 150 ? next++ : 1 + random.nextInt(next - 1);
                    ObjectNode record = record(id, random);
                    expected.put(id, record);
                    batch.add(record.toString());
                }
                harness.insert(batch);
                for (int i = 0; i < 10; i++) {
                    int id = 1 + random.nextInt(next - 1);
                    harness.delete(id);
                    expected.remove(id);
                }
                if (random.nextInt(10) < 6) {
                    harness.flush();
                }
                if (round == 3 || round == 6) {
                    harness.mergeAll();
                }
                for (int i = 0; i < 20; i++) {
                    int from = 1 + random.nextInt(next - 1);
                    assertEquals("seed " + seed + ", round " + round + ", from " + from,
                            new ArrayList<>(expected.tailMap(from).values()), read(harness.scanFrom(from)));
                }
            }
        }
    }

    private static List<JsonNode> read(List<String> records) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String record : records) {
            out.add(MAPPER.readTree(record));
        }
        return out;
    }

    /** A record of a few fields drawn from a small set, each a scalar, a null, an object or an array. */
    private static ObjectNode record(int id, Random random) {
        ObjectNode record = MAPPER.createObjectNode().put("id", id);
        for (int i = random.nextInt(5); i > 0; i--) {
            String name = "f" + random.nextInt(8);
            switch (random.nextInt(4)) {
                case 0:
                    record.put(name, random.nextInt(100));
                    break;
                case 1:
                    record.put(name, "v" + random.nextInt(10));
                    break;
                case 2:
                    ObjectNode object = record.putObject(name);
                    for (int j = random.nextInt(3); j > 0; j--) {
                        object.put("g" + random.nextInt(4), random.nextInt(10));
                    }
                    break;
                default:
                    ArrayNode array = record.putArray(name);
                    for (int j = random.nextInt(3); j > 0; j--) {
                        array.add(random.nextInt(10));
                    }
            }
        }
        return record;
    }
}
