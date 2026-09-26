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
package org.apache.hyracks.control.common.config;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class OptionTypesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void booleanRejectsAnythingButTrueOrFalse() {
        assertEquals(true, OptionTypes.BOOLEAN.parse("true"));
        assertEquals(true, OptionTypes.BOOLEAN.parse(" TRUE "));
        assertEquals(false, OptionTypes.BOOLEAN.parse("False"));
        for (String bad : new String[] { "ture", "yes", "1", "", "null" }) {
            assertThrows(bad, IllegalArgumentException.class, () -> OptionTypes.BOOLEAN.parse(bad));
        }
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.BOOLEAN.parse((String) null));
        assertEquals(true, OptionTypes.BOOLEAN.parse(BooleanNode.TRUE));
        assertEquals(false, OptionTypes.BOOLEAN.parse(new TextNode("false")));
        assertNull(OptionTypes.BOOLEAN.parse(NullNode.getInstance()));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.BOOLEAN.parse(IntNode.valueOf(1)));
    }

    @Test
    public void rangedIntegerChecksJsonValues() {
        assertEquals(Integer.valueOf(5), OptionTypes.POSITIVE_INTEGER.parse(IntNode.valueOf(5)));
        assertEquals(Integer.valueOf(5), OptionTypes.POSITIVE_INTEGER.parse(new TextNode("5")));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.POSITIVE_INTEGER.parse(IntNode.valueOf(0)));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.NONNEGATIVE_INTEGER.parse(IntNode.valueOf(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> OptionTypes.getRangedIntegerType(1, 10).parse(IntNode.valueOf(11)));
    }

    @Test
    public void integerJsonIsNotCoerced() {
        IOptionType<Integer> type = OptionTypes.INTEGER;
        assertThrows(IllegalArgumentException.class, () -> type.parse(new TextNode("abc")));
        assertThrows(IllegalArgumentException.class, () -> type.parse(DoubleNode.valueOf(1.9)));
        assertThrows(IllegalArgumentException.class, () -> type.parse(LongNode.valueOf(1L << 40)));
        assertThrows(IllegalArgumentException.class, () -> type.parse(BooleanNode.TRUE));
        assertNull(type.parse(NullNode.getInstance()));
    }

    @Test
    public void rangedLongChecksJsonValues() {
        IOptionType<Long> type = OptionTypes.getRangedLongType(1, Long.MAX_VALUE);
        assertEquals(Long.valueOf(7), type.parse(LongNode.valueOf(7)));
        assertThrows(IllegalArgumentException.class, () -> type.parse(LongNode.valueOf(0)));
        assertThrows(IllegalArgumentException.class, () -> type.parse("-1"));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.LONG.parse(new TextNode("abc")));
    }

    @Test
    public void shortJsonIsCheckedBeforeNarrowing() {
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.SHORT.parse(IntNode.valueOf(40000)));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.SHORT.parse(LongNode.valueOf(1L << 32)));
        assertEquals(Short.valueOf((short) 16), OptionTypes.SHORT.parse(new TextNode("0x10")));
    }

    @Test
    public void doubleMustBeFinite() {
        for (String bad : new String[] { "NaN", "Infinity", "-Infinity" }) {
            assertThrows(bad, IllegalArgumentException.class, () -> OptionTypes.DOUBLE.parse(bad));
        }
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.DOUBLE.parse(new TextNode("NaN")));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.DOUBLE.parse(BooleanNode.TRUE));
        assertEquals(0.5d, OptionTypes.DOUBLE.parse(DoubleNode.valueOf(0.5)), 0d);
        IOptionType<Double> ranged = OptionTypes.getRangedDoubleType(0, 1);
        assertThrows(IllegalArgumentException.class, () -> ranged.parse(DoubleNode.valueOf(1.5)));
        assertThrows(IllegalArgumentException.class, () -> ranged.parse(new TextNode("Infinity")));
    }

    @Test
    public void rangedByteUnitRejectsOutOfRange() {
        IOptionType<Integer> type = OptionTypes.getRangedIntegerByteUnit(1024, Integer.MAX_VALUE);
        assertEquals(Integer.valueOf(4096), type.parse("4kb"));
        assertThrows(IllegalArgumentException.class, () -> type.parse("512"));
        assertThrows(IllegalArgumentException.class, () -> type.parse(new TextNode("0")));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.POSITIVE_LONG_BYTE_UNIT.parse("-1GB"));
    }

    @Test
    public void intArrayRejectsGarbageTokens() throws Exception {
        assertArrayEquals(new int[] { 1, 2 }, OptionTypes.INT_ARRAY.parse("1, 2"));
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.INT_ARRAY.parse("1,x"));
        JsonNode array = MAPPER.readTree("[1, 2.5]");
        assertThrows(IllegalArgumentException.class, () -> OptionTypes.INT_ARRAY.parse(array));
    }

    @Test
    public void humanReadableSizeCoversExbibytes() {
        assertEquals("1 EiB", StorageUtil.toHumanReadableSize(1L << 60));
        assertEquals("8.00 EiB", StorageUtil.toHumanReadableSize(Long.MAX_VALUE).replace(',', '.'));
    }
}
