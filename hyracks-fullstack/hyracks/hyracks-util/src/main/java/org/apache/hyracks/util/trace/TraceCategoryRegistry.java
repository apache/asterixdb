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

public class TraceCategoryRegistry implements ITraceCategoryRegistry {

    private final Map<String, Long> categories = Collections.synchronizedMap(new HashMap<>());
    private final String[] names = new String[NO_CATEGORIES];
    private int bitPos = 0;

    public TraceCategoryRegistry() {
        categories.put(CATEGORIES_ALL_NAME, ITraceCategoryRegistry.CATEGORIES_ALL);
    }

    @Override
    public long get(String name) {
        return categories.computeIfAbsent(name, this::nextCode);
    }

    private synchronized long nextCode(String name) {
        if (bitPos > NO_CATEGORIES - 1) {
            throw new IllegalStateException("Cannot add category " + name);
        }
        names[bitPos] = name;
        return 1L << bitPos++;
    }

    @Override
    public String getName(long categoryCode) {
        if (CATEGORIES_ALL == categoryCode) {
            return CATEGORIES_ALL_NAME;
        }
        if (categoryCode == 0) {
            throw new IllegalArgumentException("Illegal category code " + categoryCode);
        }
        int postition = mostSignificantBit(categoryCode);
        if (postition >= bitPos) {
            throw new IllegalArgumentException("No category for code " + categoryCode);
        }
        return nameAt(postition);
    }

    public String nameAt(int n) {
        return names[n];
    }

    public static int mostSignificantBit(long n) {
        int pos = -1;
        while (n != 0) {
            pos++;
            n = n >>> 1;
        }
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int pos = 0; pos < NO_CATEGORIES; ++pos) {
            long categoryCode = 1L << pos;
            String name = nameAt(pos);
            if (name == null) {
                continue;
            }
            String codeString = Long.toBinaryString(categoryCode);
            sb.append(name).append(" -> ").append(codeString).append(' ');
        }
        return sb.toString();
    }
}
