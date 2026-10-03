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
package org.apache.asterix.external.util.iceberg;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * Corpus and oracle for the string-ordering pruning tests: {@link VariantBoundsStringOrderTest} here, and
 * {@code IcebergStringPredicatePushdownTest} in asterix-metadata, which reaches it through this module's test-jar.
 * <p>
 * The query engine orders strings by UTF-16 code unit ({@code UTF8StringPointable.compare}), while Iceberg records
 * and compares string bounds by code point ({@code Comparators.charSequences}). The two orders agree everywhere
 * except where a supplementary character (stored as a surrogate pair, U+D800..U+DFFF) meets a BMP character in
 * U+E000..U+FFFF at the first differing position. The corpus is built around that boundary, plus truncation (bounds
 * are cut at 16 code points) and the empty string. The oracle is the engine's own comparator, never Java's, so a
 * test's notion of "this row matches" is exactly what the query would return.
 */
public final class StringOrderCorpus {

    /** Operators the Iceberg filter builder pushes for a string literal. */
    public enum Op {
        EQ,
        NEQ,
        LT,
        LTEQ,
        GT,
        GTEQ,
        STARTS_WITH
    }

    public static final List<String> VALUES = List.of(
            // ASCII and the empty string
            "", "a", "z",
            // BMP below the surrogate range: both orders agree here
            "é", "中", "퟿",
            // BMP above the surrogate range: sorts AFTER a surrogate pair in UTF-16, BEFORE it by code point
            "", "�", "￿",
            // supplementary characters, i.e. surrogate pairs
            cp(0x10000), cp(0x1F600), cp(0x10FFFF),
            // the divergence one position in
            "a￿", "a" + cp(0x1F600),
            // longer than the 16-code-point bound truncation, diverging right at the cut
            "x".repeat(15) + cp(0x1F600) + "tail", "x".repeat(15) + "￿" + "tail", "x".repeat(20));

    private StringOrderCorpus() {
    }

    static String cp(int codePoint) {
        return new String(Character.toChars(codePoint));
    }

    /** Every single value, then every unordered pair: files of one and of two rows. */
    public static List<List<String>> fileLayouts() {
        List<List<String>> layouts = new ArrayList<>();
        for (String v : VALUES) {
            layouts.add(List.of(v));
        }
        for (int i = 0; i < VALUES.size(); i++) {
            for (int j = i + 1; j < VALUES.size(); j++) {
                layouts.add(List.of(VALUES.get(i), VALUES.get(j)));
            }
        }
        return layouts;
    }

    /** The engine's comparison: what the query evaluates each row with after pushdown has chosen the files. */
    public static int engineCompare(String a, String b) {
        return UTF8StringUtil.compareTo(UTF8StringUtil.writeStringToBytes(a), 0, UTF8StringUtil.writeStringToBytes(b),
                0);
    }

    public static boolean engineMatches(String value, Op op, String literal) {
        switch (op) {
            case EQ:
                return engineCompare(value, literal) == 0;
            case NEQ:
                return engineCompare(value, literal) != 0;
            case LT:
                return engineCompare(value, literal) < 0;
            case LTEQ:
                return engineCompare(value, literal) <= 0;
            case GT:
                return engineCompare(value, literal) > 0;
            case GTEQ:
                return engineCompare(value, literal) >= 0;
            case STARTS_WITH:
                return value.startsWith(literal);
            default:
                throw new IllegalArgumentException(op.name());
        }
    }

    /** {@code column <op> literal} as a plain Iceberg expression, the input the variant rewrite starts from. */
    public static Expression pushed(String column, Op op, String literal) {
        switch (op) {
            case EQ:
                return Expressions.equal(column, literal);
            case NEQ:
                return Expressions.notEqual(column, literal);
            case LT:
                return Expressions.lessThan(column, literal);
            case LTEQ:
                return Expressions.lessThanOrEqual(column, literal);
            case GT:
                return Expressions.greaterThan(column, literal);
            case GTEQ:
                return Expressions.greaterThanOrEqual(column, literal);
            case STARTS_WITH:
                return Expressions.startsWith(column, literal);
            default:
                throw new IllegalArgumentException(op.name());
        }
    }

    /**
     * Whether exact (untruncated) bounds alone prove no row can match: the literal falls outside [min, max]. For the
     * range operators that is the same as "no row matches"; for EQ a literal strictly between two values matches
     * nothing yet cannot be excluded by bounds, so it is not counted as prunable.
     */
    public static boolean boundsExclude(List<String> values, Op op, String literal) {
        String min = values.get(0);
        String max = values.get(0);
        for (String v : values) {
            min = engineCompare(v, min) < 0 ? v : min;
            max = engineCompare(v, max) > 0 ? v : max;
        }
        switch (op) {
            case EQ:
                return engineCompare(literal, min) < 0 || engineCompare(literal, max) > 0;
            case LT:
                return engineCompare(min, literal) >= 0;
            case LTEQ:
                return engineCompare(min, literal) > 0;
            case GT:
                return engineCompare(max, literal) <= 0;
            case GTEQ:
                return engineCompare(max, literal) < 0;
            default:
                return false;
        }
    }

    /** True when the two orders provably agree for every comparison among these strings and are exact (untruncated). */
    public static boolean orderInsensitive(List<String> values, String literal) {
        for (String s : values) {
            if (!belowSurrogates(s) || s.codePointCount(0, s.length()) > 16) {
                return false;
            }
        }
        return belowSurrogates(literal) && literal.codePointCount(0, literal.length()) <= 16;
    }

    private static boolean belowSurrogates(String s) {
        return s.chars().allMatch(c -> c < 0xD800);
    }

    public static String describe(String s) {
        if (s.isEmpty()) {
            return "\"\"";
        }
        StringBuilder sb = new StringBuilder();
        int asciiRun = 0;
        for (int i = 0; i < s.length();) {
            int c = s.codePointAt(i);
            if (c >= 0x20 && c < 0x7F) {
                asciiRun++;
            } else {
                flush(sb, s, i, asciiRun);
                asciiRun = 0;
                sb.append(String.format("U+%04X ", c));
            }
            i += Character.charCount(c);
        }
        flush(sb, s, s.length(), asciiRun);
        return sb.toString().trim();
    }

    private static void flush(StringBuilder sb, String s, int end, int asciiRun) {
        if (asciiRun > 0) {
            String run = s.substring(end - asciiRun, end);
            sb.append(asciiRun > 4 ? "'" + run.charAt(0) + "'x" + asciiRun + " " : "'" + run + "' ");
        }
    }
}
