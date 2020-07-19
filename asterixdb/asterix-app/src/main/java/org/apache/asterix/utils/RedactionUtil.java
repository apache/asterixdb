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
package org.apache.asterix.utils;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.SECRET_ACCESS_KEY_FIELD_NAME;

import java.util.regex.Pattern;

import org.apache.hyracks.util.ILogRedactor;

public class RedactionUtil {
    private RedactionUtil() {
        throw new AssertionError("do not instantiate");
    }

    private static final Pattern STATEMENT_PATTERN =
            Pattern.compile("(" + SECRET_ACCESS_KEY_FIELD_NAME + ").*", CASE_INSENSITIVE | DOTALL);
    private static final String STATEMENT_REPLACEMENT = "$1...<redacted sensitive data>";

    public static final ILogRedactor LOG_REDACTOR = new ILogRedactor() {
        @Override
        public String userData(String text) {
            return text;
        }

        @Override
        public String statement(String text) {
            return STATEMENT_PATTERN.matcher(text).replaceFirst(STATEMENT_REPLACEMENT);
        }

        @Override
        public String unredactUserData(String text) {
            return text;
        }
    };
}
