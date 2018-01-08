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
package org.apache.hyracks.dataflow.common.utils;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class NormalizedKeyUtils {

    private NormalizedKeyUtils() {
    }

    public static int compareNormalizeKeys(int[] keys1, int start1, int[] keys2, int start2, int length) {
        for (int i = 0; i < length; i++) {
            int key1 = keys1[start1 + i];
            int key2 = keys2[start2 + i];
            if (key1 != key2) {
                return (((key1) & 0xffffffffL) < ((key2) & 0xffffffffL)) ? -1 : 1;
            }
        }
        return 0;
    }

    public static int getDecisivePrefixLength(INormalizedKeyComputerFactory[] keyNormalizerFactories) {
        if (keyNormalizerFactories == null) {
            return 0;
        }
        for (int i = 0; i < keyNormalizerFactories.length; i++) {
            if (!keyNormalizerFactories[i].getNormalizedKeyProperties().isDecisive()) {
                return i;
            }
        }
        return keyNormalizerFactories.length;
    }

    public static void putLongIntoNormalizedKeys(int[] normalizedKeys, int keyStart, long key) {
        int high = (int) (key >> 32);
        normalizedKeys[keyStart] = high;
        int low = (int) key;
        normalizedKeys[keyStart + 1] = low;
    }

    public static int[] createNormalizedKeyArray(INormalizedKeyComputer normalizer) {
        if (normalizer == null) {
            return null; //NOSONAR
        }
        return new int[normalizer.getNormalizedKeyProperties().getNormalizedKeyLength()];
    }

}
