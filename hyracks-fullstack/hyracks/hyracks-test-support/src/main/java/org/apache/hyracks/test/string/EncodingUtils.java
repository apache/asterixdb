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
package org.apache.hyracks.test.string;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EncodingUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    private EncodingUtils() {
    }

    // duplicated in [asterix-app]TestExecutor.java as transitive dependencies on test-jars are not handled correctly
    public static boolean canEncodeDecode(String input, Charset charset, boolean quiet) {
        try {
            if (input.equals(new String(input.getBytes(charset), charset))) {
                if (!input.equals(charset.decode(charset.encode(CharBuffer.wrap(input))).toString())) {
                    // workaround for https://bugs.openjdk.java.net/browse/JDK-6392670 and similar
                    if (!quiet) {
                        LOGGER.info("cannot encode / decode {} with {} using CharBuffer.wrap(<String>)", input,
                                charset.displayName());
                    }
                } else {
                    return true;
                }
            }
            if (!quiet) {
                LOGGER.info("cannot encode / decode {} with {}", input, charset.displayName());
            }
        } catch (Exception e) {
            if (!quiet) {
                LOGGER.info("cannot encode / decode {} with {}, got exception ({})", input, charset.displayName(),
                        String.valueOf(e));
            }
        }
        return false;
    }

    public static Set<Charset> getLegalCharsetsFor(String input) {
        return Charset.availableCharsets().values().stream().filter(Charset::canEncode)
                .filter(test -> canEncodeDecode(input, test, false)).collect(Collectors.toSet());
    }
}
