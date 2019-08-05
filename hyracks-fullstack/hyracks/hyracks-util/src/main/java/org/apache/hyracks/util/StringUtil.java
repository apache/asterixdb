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
package org.apache.hyracks.util;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.text.WordUtils;

public class StringUtil {
    private static final Map<String, String> CAMEL_CACHE = Collections.synchronizedMap(new LRUMap<>(1024));
    private static final Pattern SEPARATORS_PATTERN = Pattern.compile("[_\\-\\s]");

    private StringUtil() {
    }

    public static String toCamelCase(String input) {
        return CAMEL_CACHE.computeIfAbsent(input, s -> SEPARATORS_PATTERN
                .matcher(WordUtils.capitalize("z" + s.toLowerCase(), '_', '-', ' ').substring(1)).replaceAll(""));
    }

    public static String join(Object[] objects, String separator, String quote) {
        if (objects == null || objects.length == 0) {
            return "";
        }
        int length = objects.length;
        String str0 = String.valueOf(objects[0]);
        StringBuilder sb = new StringBuilder((str0.length() + 3) * length);
        sb.append(quote).append(str0).append(quote);
        for (int i = 1; i < length; i++) {
            sb.append(separator).append(quote).append(objects[i]).append(quote);
        }
        return sb.toString();
    }
}
