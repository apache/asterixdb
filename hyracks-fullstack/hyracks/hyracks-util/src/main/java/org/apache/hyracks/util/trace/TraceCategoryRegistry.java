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

package org.apache.hyracks.util.trace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TraceCategoryRegistry implements ITraceCategoryRegistry {

    private Map<String, Long> categories = Collections.synchronizedMap(new HashMap<>());
    private int bitPos = 0;

    public TraceCategoryRegistry() {
        categories.put("*", ITraceCategoryRegistry.CATEGORIES_ALL);
    }

    @Override
    public long get(String name) {
        return categories.computeIfAbsent(name, this::nextCode);
    }

    private long nextCode(String name) {
        if (bitPos > NO_CATEGORIES - 1) {
            throw new IllegalStateException("Cannot add category " + name);
        }
        return 1L << bitPos++;
    }

    @Override
    public long get(String... names) {
        long result = 0;
        for (String name : names) {
            result |= get(name);
        }
        return result;
    }

    private Optional<Map.Entry<String, Long>> findEntry(long categoryCode) {
        return categories.entrySet().stream().filter(e -> e.getValue() == categoryCode).findFirst();
    }

    @Override
    public String getName(long categoryCode) {
        Optional<Map.Entry<String, Long>> entry = findEntry(categoryCode);
        if (!entry.isPresent()) {
            throw new IllegalArgumentException("No category for code " + categoryCode);
        }
        return entry.get().getKey();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int pos = 0; pos < NO_CATEGORIES; ++pos) {
            long categoryCode = 1L << pos;
            Optional<Map.Entry<String, Long>> entry = findEntry(categoryCode);
            if (!entry.isPresent()) {
                continue;
            }
            String name = entry.get().getKey();
            String codeString = Long.toBinaryString(categoryCode);
            sb.append(name).append(" -> ").append(codeString).append(' ');
        }
        return sb.toString();
    }
}
