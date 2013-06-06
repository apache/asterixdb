/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.types;

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
    DAYTIMEDURATION(37);

    private byte value;

    private ATypeTag(int value) {
        this.value = (byte) value;
    }

    @Override
    public byte serialize() {
        return value;
    }

    public final static int TYPE_COUNT = ATypeTag.values().length;

}