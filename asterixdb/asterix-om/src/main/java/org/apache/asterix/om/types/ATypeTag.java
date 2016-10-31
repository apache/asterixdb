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

package org.apache.asterix.om.types;

import java.util.ArrayList;
import java.util.List;

/**
 * There is a unique tag for each primitive type and for each kind of
 * non-primitive type in the object model
 */
public enum ATypeTag implements IEnumSerializer {
    INT8(1),
    INT16(2),
    INT32(3),
    INT64(4),
    UINT8(5),
    UINT16(6),
    UINT32(7),
    UINT64(8),
    BINARY(9),
    BITARRAY(10),
    FLOAT(11),
    DOUBLE(12),
    STRING(13),
    MISSING(14),
    BOOLEAN(15),
    DATETIME(16),
    DATE(17),
    TIME(18),
    DURATION(19),
    POINT(20),
    POINT3D(21),
    ORDEREDLIST(22),
    UNORDEREDLIST(23),
    RECORD(24),
    SPARSERECORD(25),
    UNION(26),
    ENUM(27),
    TYPE(28),
    ANY(29),
    LINE(30),
    POLYGON(31),
    CIRCLE(32),
    RECTANGLE(33),
    INTERVAL(34),
    SYSTEM_NULL(35),
    YEARMONTHDURATION(36),
    DAYTIMEDURATION(37),
    UUID(38),
    SHORTWITHOUTTYPEINFO(40),
    NULL(41);

    /*
     * Serialized Tags begin
     */
    public static final byte SERIALIZED_STRING_TYPE_TAG = STRING.serialize();
    public static final byte SERIALIZED_MISSING_TYPE_TAG = MISSING.serialize();
    public static final byte SERIALIZED_NULL_TYPE_TAG = NULL.serialize();
    public static final byte SERIALIZED_DOUBLE_TYPE_TAG = DOUBLE.serialize();
    public static final byte SERIALIZED_RECORD_TYPE_TAG = RECORD.serialize();
    public static final byte SERIALIZED_INT32_TYPE_TAG = INT32.serialize();
    public static final byte SERIALIZED_ORDEREDLIST_TYPE_TAG = ORDEREDLIST.serialize();
    public static final byte SERIALIZED_UNORDEREDLIST_TYPE_TAG = UNORDEREDLIST.serialize();
    public static final byte SERIALIZED_POLYGON_TYPE_TAG = POLYGON.serialize();
    public static final byte SERIALIZED_DATE_TYPE_TAG = DATE.serialize();
    public static final byte SERIALIZED_TIME_TYPE_TAG = TIME.serialize();
    public static final byte SERIALIZED_DATETIME_TYPE_TAG = DATETIME.serialize();
    public static final byte SERIALIZED_SYSTEM_NULL_TYPE_TAG = SYSTEM_NULL.serialize();
    public static final byte SERIALIZED_DURATION_TYPE_TAG = DURATION.serialize();
    public static final byte SERIALIZED_DAY_TIME_DURATION_TYPE_TAG = DAYTIMEDURATION.serialize();
    public static final byte SERIALIZED_POINT_TYPE_TAG = POINT.serialize();
    public static final byte SERIALIZED_INTERVAL_TYPE_TAG = INTERVAL.serialize();
    public static final byte SERIALIZED_CIRCLE_TYPE_TAG = CIRCLE.serialize();
    public static final byte SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG = YEARMONTHDURATION.serialize();
    public static final byte SERIALIZED_LINE_TYPE_TAG = LINE.serialize();
    public static final byte SERIALIZED_RECTANGLE_TYPE_TAG = RECTANGLE.serialize();
    public static final byte SERIALIZED_BOOLEAN_TYPE_TAG = BOOLEAN.serialize();
    public static final byte SERIALIZED_INT8_TYPE_TAG = INT8.serialize();
    public static final byte SERIALIZED_INT16_TYPE_TAG = INT16.serialize();
    public static final byte SERIALIZED_INT64_TYPE_TAG = INT64.serialize();
    public static final byte SERIALIZED_FLOAT_TYPE_TAG = FLOAT.serialize();
    public static final byte SERIALIZED_BINARY_TYPE_TAG = BINARY.serialize();
    public static final byte SERIALIZED_UUID_TYPE_TAG = UUID.serialize();

    /*
     * Serialized Tags end
     */
    public static final int TYPE_COUNT = ATypeTag.values().length;
    private byte value;
    public static final ATypeTag[] VALUE_TYPE_MAPPING;

    static {
        List<ATypeTag> typeList = new ArrayList<>();
        for (ATypeTag tt : values()) {
            int index = tt.value;
            while (typeList.size() <= index) {
                typeList.add(null);
            }
            typeList.set(index, tt);
        }
        VALUE_TYPE_MAPPING = typeList.toArray(new ATypeTag[typeList.size()]);
    }

    private ATypeTag(int value) {
        this.value = (byte) value;
    }

    @Override
    public byte serialize() {
        return value;
    }

    public boolean isDerivedType() {
        return this == ATypeTag.RECORD || this == ATypeTag.ORDEREDLIST || this == ATypeTag.UNORDEREDLIST
                || this == ATypeTag.UNION;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase();
    }

}
