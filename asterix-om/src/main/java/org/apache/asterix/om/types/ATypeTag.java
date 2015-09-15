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
 * non-primitive type in the object model.
 *
 * @author Nicola
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
    NULL(14),
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
    UUID_STRING(39),
    SHORTWITHOUTTYPEINFO(40);

    private byte value;

    private ATypeTag(int value) {
        this.value = (byte) value;
    }

    @Override
    public byte serialize() {
        return value;
    }

    public static final int TYPE_COUNT = ATypeTag.values().length;

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

    public boolean isDerivedType() {
        if (this == ATypeTag.RECORD || this == ATypeTag.ORDEREDLIST || this == ATypeTag.UNORDEREDLIST
                || this == ATypeTag.UNION)
            return true;
        return false;
    }

}