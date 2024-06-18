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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud;

import it.unimi.dsi.fastutil.longs.LongComparator;

public class IntPairUtil {
    private IntPairUtil() {
    }

    public static final LongComparator FIRST_COMPARATOR = (x, y) -> Integer.compare(getFirst(x), getFirst(y));
    public static final LongComparator SECOND_COMPARATOR = (x, y) -> Integer.compare(getSecond(x), getSecond(y));

    public static int getFirst(long pair) {
        return (int) (pair >> 32);
    }

    public static int getSecond(long pair) {
        return (int) pair;
    }

    public static long of(int first, int second) {
        return ((long) first << 32) + second;
    }
}
