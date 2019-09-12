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
package org.apache.asterix.runtime.evaluators.functions;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class StringEvaluatorUtils {

    public static int toFlag(String pattern) {
        int flag = 0;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case 's':
                    flag |= Pattern.DOTALL;
                    break;
                case 'm':
                    flag |= Pattern.MULTILINE;
                    break;
                case 'i':
                    flag |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'x':
                    flag |= Pattern.COMMENTS;
                    break;
            }
        }
        return flag;
    }

    public static UTF8StringPointable copyResetUTF8Pointable(UTF8StringPointable srcString,
            ByteArrayAccessibleOutputStream destCopyStorage, UTF8StringPointable destString) {
        destCopyStorage.reset();
        destCopyStorage.write(srcString.getByteArray(), srcString.getStartOffset(),
                srcString.getMetaDataLength() + srcString.getUTF8Length());
        destString.set(destCopyStorage.getByteArray(), 0, destCopyStorage.size());
        return destString;
    }

    static char[] reservedRegexChars =
            new char[] { '\\', '(', ')', '[', ']', '{', '}', '.', '^', '$', '*', '|', '+', '?' };

    static {
        Arrays.sort(reservedRegexChars);
    }

    public static String toRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && (i < pattern.length() - 1)
                    && (pattern.charAt(i + 1) == '_' || pattern.charAt(i + 1) == '%')) {
                sb.append(pattern.charAt(i + 1));
                ++i;
            } else if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append(".");
            } else {
                if (Arrays.binarySearch(reservedRegexChars, c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
