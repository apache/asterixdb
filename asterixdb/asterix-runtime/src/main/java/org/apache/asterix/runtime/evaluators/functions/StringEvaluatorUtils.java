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

public final class StringEvaluatorUtils {

    private StringEvaluatorUtils() {
    }

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

    public static final char[] RESERVED_REGEX_CHARS = new char[] { '\\', '(', ')', '[', ']', '{', '}', '.', '^', '$',
            '*', '|', '+', '?', '<', '>', '-', '=', '!' };

    static {
        Arrays.sort(RESERVED_REGEX_CHARS);
    }
}
