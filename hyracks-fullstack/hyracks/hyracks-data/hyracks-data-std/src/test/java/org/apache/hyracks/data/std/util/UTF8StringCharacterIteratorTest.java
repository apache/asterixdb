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

package org.apache.hyracks.data.std.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.string.UTF8StringSample;
import org.junit.Test;

public class UTF8StringCharacterIteratorTest {

    private UTF8StringCharacterIterator iterator = new UTF8StringCharacterIterator();

    private void testEachIterator(String testString) {
        UTF8StringPointable ptr = UTF8StringPointable.generateUTF8Pointable(testString);
        iterator.reset(ptr);
        for (char ch : testString.toCharArray()) {
            assertTrue(iterator.hasNext());
            assertEquals(ch, iterator.next());
        }
        assertFalse(iterator.hasNext());

        iterator.reset();
        for (char ch : testString.toCharArray()) {
            assertTrue(iterator.hasNext());
            assertEquals(ch, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator() {
        testEachIterator(UTF8StringSample.EMPTY_STRING);
        testEachIterator(UTF8StringSample.STRING_UTF8_MIX);
        testEachIterator(UTF8StringSample.STRING_LEN_128);
        testEachIterator(UTF8StringSample.STRING_LEN_128);
    }
}
