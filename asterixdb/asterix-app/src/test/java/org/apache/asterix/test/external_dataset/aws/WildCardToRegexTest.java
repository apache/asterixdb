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

package org.apache.asterix.test.external_dataset.aws;

import static org.apache.asterix.external.util.ExternalDataUtils.patternToRegex;

import java.util.regex.Pattern;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

import junit.framework.TestCase;

public class WildCardToRegexTest extends TestCase {

    @Test
    public void test() throws HyracksDataException {
        String result = patternToRegex("*?[abc]");
        assertEquals(".*.[abc]", result);
        Pattern.compile(result);

        result = patternToRegex("*?[!@#$%^&*()+<>|=!{}.]");
        assertEquals(".*.[^@#\\$%\\^&\\*\\(\\)\\+\\<\\>\\|\\=\\!\\{\\}\\.]", result);
        Pattern.compile(result);

        result = patternToRegex("**??[[a-z*0-9]]");
        assertEquals(".*.*..[\\[a-z\\*0-9]\\]", result);
        Pattern.compile(result);

        result = patternToRegex("**??[[a-z*0-9]]");
        assertEquals(".*.*..[\\[a-z\\*0-9]\\]", result);
        Pattern.compile(result);

        result = patternToRegex("*?[!abc]");
        assertEquals(".*.[^abc]", result);
        Pattern.compile(result);

        result = patternToRegex("[!]abc");
        assertEquals("\\[\\!\\]abc", result);
        Pattern.compile(result);

        result = patternToRegex("[!]abc]");
        assertEquals("[^\\]abc]", result);
        Pattern.compile(result);

        result = patternToRegex("[]]");
        assertEquals("[\\]]", result);
        Pattern.compile(result);

        result = patternToRegex("[]abcd");
        assertEquals("\\[\\]abcd", result);
        Pattern.compile(result);

        result = patternToRegex("[]abcd]");
        assertEquals("[\\]abcd]", result);
        Pattern.compile(result);

        result = patternToRegex("[^]");
        assertEquals("[\\^]", result);
        Pattern.compile(result);

        result = patternToRegex("[^]]");
        assertEquals("[\\^]\\]", result);
        Pattern.compile(result);

        result = patternToRegex("[!]");
        assertEquals("\\[\\!\\]", result);
        Pattern.compile(result);

        result = patternToRegex("[!]]");
        assertEquals("[^\\]]", result);
        Pattern.compile(result);

        result = patternToRegex("[][!][^]]]]*[![*a-zA--&&^$||0-9B$\\\\*&&]*&&[^a-b||0--9][[[");
        assertEquals(
                "[\\]\\[\\!][\\^]\\]\\]\\].*[^\\[\\*a-zA\\-\\-\\&\\&\\^\\$\\|\\|0-9B\\$\\\\\\\\\\*\\&\\&].*&&[\\^a-b\\|\\|0\\-\\-9]\\[\\[\\[",
                result);
        Pattern.compile(result);
    }
}
