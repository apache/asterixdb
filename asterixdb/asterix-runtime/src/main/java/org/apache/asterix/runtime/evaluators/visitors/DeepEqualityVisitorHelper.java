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
package org.apache.asterix.runtime.evaluators.visitors;

import java.util.Arrays;

import org.apache.asterix.dataflow.data.nontagged.comparators.ListItemBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.hash.ListItemBinaryHashFunctionFactory;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.data.std.util.BinaryEntry;

public class DeepEqualityVisitorHelper {
    // Default values
    public static final int TABLE_SIZE = 100;
    public static final int TABLE_FRAME_SIZE = 32768;

    private final ListItemBinaryHashFunctionFactory listItemBinaryHashFunctionFactory =
            ListItemBinaryHashFunctionFactory.INSTANCE;
    private final ListItemBinaryComparatorFactory listItemBinaryComparatorFactory =
            ListItemBinaryComparatorFactory.INSTANCE;

    private final IBinaryHashFunction putHashFunc = listItemBinaryHashFunctionFactory.createBinaryHashFunction();
    private final IBinaryHashFunction getHashFunc = listItemBinaryHashFunctionFactory.createBinaryHashFunction();
    private IBinaryComparator cmp = listItemBinaryComparatorFactory.createBinaryComparator();
    private BinaryHashMap hashMap = null;

    public BinaryHashMap initializeHashMap(BinaryEntry valEntry) {
        return initializeHashMap(0, 0, valEntry);
    }

    public BinaryHashMap initializeHashMap(int tableSize, int tableFrameSize, BinaryEntry valEntry) {
        if (tableFrameSize != 0 && tableSize != 0) {
            hashMap = new BinaryHashMap(tableSize, tableFrameSize, putHashFunc, getHashFunc, cmp);
        } else {
            hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, putHashFunc, getHashFunc, cmp);
        }

        byte[] emptyValBuf = new byte[8];
        Arrays.fill(emptyValBuf, (byte) 0);
        valEntry.set(emptyValBuf, 0, 8);
        return hashMap;
    }

}
