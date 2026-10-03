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

/**
 * Where the engine's string order and code-point order can disagree, for pushing string predicates into readers that
 * filter by code point: parquet-mr row-group statistics (unsigned UTF-8 bytes) and Iceberg file and row-group bounds.
 * <p>
 * The engine orders strings by UTF-16 code unit. The two orders disagree only where a character in U+E000..U+FFFF
 * meets a supplementary character, whose UTF-16 form begins with a surrogate below U+E000. A range literal holding no
 * character at or above U+D800 is on the same side of every stored value in both orders -- at the first position they
 * differ its character is below every surrogate -- so only a literal that does hold one needs declining. Equality is
 * unaffected: bounds that hold a value always span it in their own order.
 */
public final class StringOrder {

    private StringOrder() {
    }

    /** Whether a range comparison against {@code literal} could exclude a value the engine matches. */
    public static boolean rangeOrdersDifferently(CharSequence literal) {
        for (int i = 0; i < literal.length(); i++) {
            if (literal.charAt(i) >= '\uD800') {
                return true;
            }
        }
        return false;
    }
}
