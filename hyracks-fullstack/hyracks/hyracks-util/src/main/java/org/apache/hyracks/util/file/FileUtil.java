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
package org.apache.hyracks.util.file;

import java.io.File;
import java.util.regex.Pattern;

public class FileUtil {

    private FileUtil() {
    }

    public static String joinPath(String... elements) {
        return joinPath(File.separatorChar, elements);
    }

    static String joinPath(char separatorChar, String... elements) {
        final String separator = String.valueOf(separatorChar);
        final String escapedSeparator = Pattern.quote(separator);
        String joined = String.join(separator, elements);
        if (separatorChar == '\\') {
            // preserve leading double-slash on windows (UNC)
            return joined.replaceAll("([^" + escapedSeparator + "])(" + escapedSeparator + ")+", "$1$2")
                    .replaceAll(escapedSeparator + "$", "");
        } else {
            return joined.replaceAll("(" + escapedSeparator + ")+", "$1").replaceAll(escapedSeparator + "$", "");
        }
    }
}
