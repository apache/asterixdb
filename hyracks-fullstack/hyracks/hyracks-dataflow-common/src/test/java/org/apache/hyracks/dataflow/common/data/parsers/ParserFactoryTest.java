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

package org.apache.hyracks.dataflow.common.data.parsers;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Test;

import junit.framework.TestCase;

public class ParserFactoryTest extends TestCase {

    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
    private final IValueParser integerParser = IntegerParserFactory.INSTANCE.createValueParser();
    private final IValueParser longParser = LongParserFactory.INSTANCE.createValueParser();
    private final IValueParser booleanParser = BooleanParserFactory.INSTANCE.createValueParser();
    private String chars = "";

    @Test
    public void testInteger() throws HyracksDataException {
        String number = "12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "+12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "-12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, -12, true);
        number = "  12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "  +12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "  -12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, -12, true);
        number = "12   ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "+12   ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "-12  ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, -12, true);
        number = "   12   ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "   +12   ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, 12, true);
        number = "   -12  ";
        parse(number, integerParser, storage, IntegerPointable::getInteger, -12, true);

        number = Integer.toString(Integer.MAX_VALUE);
        parse(number, integerParser, storage, IntegerPointable::getInteger, Integer.MAX_VALUE, true);
        number = Integer.toString(Integer.MIN_VALUE);
        parse(number, integerParser, storage, IntegerPointable::getInteger, Integer.MIN_VALUE, true);

        // overflow and underflow
        number = Long.toString(Integer.MAX_VALUE + 1L);
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = Long.toString(Integer.MIN_VALUE - 1L);
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);

        // invalid
        number = "a";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "12a";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "12  a";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "  a 12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "a12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "+ 12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "- 12";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
        number = "1 2";
        parse(number, integerParser, storage, IntegerPointable::getInteger, null, false);
    }

    @Test
    public void testLong() throws HyracksDataException {
        storage.reset();
        String number = "12";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "+12";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "-12";
        parse(number, longParser, storage, LongPointable::getLong, -12L, true);
        number = "  12";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "  +12";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "  -12";
        parse(number, longParser, storage, LongPointable::getLong, -12L, true);
        number = "12   ";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "+12   ";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "-12  ";
        parse(number, longParser, storage, LongPointable::getLong, -12L, true);
        number = "   12   ";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "   +12   ";
        parse(number, longParser, storage, LongPointable::getLong, 12L, true);
        number = "   -12  ";
        parse(number, longParser, storage, LongPointable::getLong, -12L, true);

        number = Long.toString(Long.MAX_VALUE);
        parse(number, longParser, storage, LongPointable::getLong, Long.MAX_VALUE, true);
        number = Long.toString(Long.MIN_VALUE);
        parse(number, longParser, storage, LongPointable::getLong, Long.MIN_VALUE, true);

        // overflow and underflow
        number = Long.toString(Long.MAX_VALUE) + "1";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = Long.toString(Long.MIN_VALUE) + "1";
        parse(number, longParser, storage, LongPointable::getLong, null, false);

        // invalid
        number = "a";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "12a";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "12  a";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "  a 12";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "+ 12";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "- 12";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
        number = "1 2";
        parse(number, longParser, storage, LongPointable::getLong, null, false);
    }

    @Test
    public void testBoolean() throws HyracksDataException {
        storage.reset();
        String bool = "true";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);
        bool = "TRUE";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);
        bool = "True";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);
        bool = "true  ";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);
        bool = "    true";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);
        bool = "  True   ";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.TRUE, true);

        bool = "false";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);
        bool = "FALSE";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);
        bool = "False";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);
        bool = "  false  ";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);
        bool = "  false";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);
        bool = "false   ";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, Boolean.FALSE, true);

        // invalid
        bool = "foo";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
        bool = "truea";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
        bool = "ffalse";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
        bool = "ffalse";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
        bool = "t rue";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
        bool = "true  a";
        parse(bool, booleanParser, storage, BooleanPointable::getBoolean, null, false);
    }

    private <T> void parse(String test, IValueParser parser, ArrayBackedValueStorage storage, Getter<T> getter,
            T expectedVal, boolean expectedResult) throws HyracksDataException {
        int oldSize = storage.getLength();
        int start = storage.getLength();
        int stringStart = chars.length();
        chars = chars + test;
        int stringLength = chars.length() - stringStart;
        boolean result = parser.parse(chars.toCharArray(), stringStart, stringLength, storage.getDataOutput());
        int newSize = storage.getLength();
        if (!result) {
            assertEquals(oldSize, newSize);
        } else {
            assertEquals(expectedVal, getter.get(storage.getByteArray(), start));
        }
        assertEquals(expectedResult, result);

    }

    @FunctionalInterface
    private interface Getter<T> {
        T get(byte[] bytes, int start);
    }
}
