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
package org.apache.asterix.external.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class StringOrderTest {

    /** Boundary characters on each side of the surrogate range and of U+E000. */
    private static final List<String> ALPHABET =
            List.of("a", "z", "é", "中", "퟾", "퟿", "", "＄", "￿", cp(0x10000), cp(0x1F600), cp(0x10FFFF));

    private static String cp(int codePoint) {
        return new String(Character.toChars(codePoint));
    }

    @Test
    public void flagsOnlyLiteralsWithACharacterAtOrAboveD800() {
        Assert.assertFalse(StringOrder.rangeOrdersDifferently(""));
        Assert.assertFalse(StringOrder.rangeOrdersDifferently("abc"));
        Assert.assertFalse(StringOrder.rangeOrdersDifferently("café"));
        Assert.assertFalse(StringOrder.rangeOrdersDifferently("中文"));
        Assert.assertFalse(StringOrder.rangeOrdersDifferently("퟿"));
        Assert.assertTrue(StringOrder.rangeOrdersDifferently(""));
        Assert.assertTrue(StringOrder.rangeOrdersDifferently("￿"));
        Assert.assertTrue(StringOrder.rangeOrdersDifferently(cp(0x1F600)));
        Assert.assertTrue(StringOrder.rangeOrdersDifferently("abc＄"));
    }

    /**
     * The claim the guard rests on: for a literal it does not flag, the engine's order and code-point order put every
     * value on the same side of it. Checked exhaustively for every string of up to two characters over the alphabet,
     * against the engine's own comparator.
     */
    @Test
    public void unflaggedLiteralsCompareTheSameInBothOrders() {
        List<String> strings = strings(2);
        int checked = 0;
        List<String> disagreements = new ArrayList<>();
        for (String literal : strings) {
            if (StringOrder.rangeOrdersDifferently(literal)) {
                continue;
            }
            byte[] literalBytes = UTF8StringUtil.writeStringToBytes(literal);
            for (String value : strings) {
                int engine = Integer
                        .signum(UTF8StringUtil.compareTo(UTF8StringUtil.writeStringToBytes(value), 0, literalBytes, 0));
                int codePoint = Integer.signum(compareByCodePoint(value, literal));
                checked++;
                if (engine != codePoint && disagreements.size() < 10) {
                    disagreements.add(describe(value) + " vs " + describe(literal));
                }
            }
        }
        Assert.assertTrue("orders disagree for unflagged literals: " + disagreements, disagreements.isEmpty());
        Assert.assertTrue(checked > 0);
    }

    /** The guard is needed: across the same strings, flagged literals do include real disagreements. */
    @Test
    public void flaggedLiteralsIncludeDisagreements() {
        List<String> strings = strings(2);
        for (String literal : strings) {
            if (!StringOrder.rangeOrdersDifferently(literal)) {
                continue;
            }
            byte[] literalBytes = UTF8StringUtil.writeStringToBytes(literal);
            for (String value : strings) {
                int engine = Integer
                        .signum(UTF8StringUtil.compareTo(UTF8StringUtil.writeStringToBytes(value), 0, literalBytes, 0));
                if (engine != Integer.signum(compareByCodePoint(value, literal))) {
                    return;
                }
            }
        }
        Assert.fail("no flagged literal is ever ordered differently, so the guard would be unnecessary");
    }

    private static List<String> strings(int maxLength) {
        List<String> result = new ArrayList<>();
        result.add("");
        List<String> previous = List.of("");
        for (int length = 1; length <= maxLength; length++) {
            List<String> next = new ArrayList<>();
            for (String prefix : previous) {
                for (String c : ALPHABET) {
                    next.add(prefix + c);
                }
            }
            result.addAll(next);
            previous = next;
        }
        return result;
    }

    private static int compareByCodePoint(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    private static String describe(String s) {
        StringBuilder sb = new StringBuilder();
        s.codePoints().forEach(c -> sb.append(String.format("U+%04X ", c)));
        return sb.length() == 0 ? "\"\"" : sb.toString().trim();
    }
}
