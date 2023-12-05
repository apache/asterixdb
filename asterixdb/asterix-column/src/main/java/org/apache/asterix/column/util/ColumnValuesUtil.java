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
package org.apache.asterix.column.util;

import org.apache.asterix.om.types.ATypeTag;

public class ColumnValuesUtil {
    private ColumnValuesUtil() {
    }

    public static int getBitWidth(int level) {
        //+1 for the null bit
        return (32 - Integer.numberOfLeadingZeros(level)) + 1;
    }

    public static int getNullMask(int level) {
        return 1 << getBitWidth(level) - 1;
    }

    public static boolean isNull(int mask, int level) {
        return (mask & level) == mask;
    }

    public static int getChildValue(int parentMask, int childMask, int level) {
        if (isNull(parentMask, level)) {
            return clearNullBit(parentMask, level) | childMask;
        }
        return level;
    }

    public static int clearNullBit(int nullBitMask, int level) {
        return (nullBitMask - 1) & level;
    }

    public static ATypeTag getNormalizedTypeTag(ATypeTag typeTag) {
        switch (typeTag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ATypeTag.BIGINT;
            default:
                return typeTag;
        }
    }
}
