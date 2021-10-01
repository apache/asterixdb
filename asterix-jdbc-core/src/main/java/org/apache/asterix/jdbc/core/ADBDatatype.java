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

package org.apache.asterix.jdbc.core;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

enum ADBDatatype {
    TINYINT(1, "int8", JDBCType.TINYINT, Byte.class),
    SMALLINT(2, "int16", JDBCType.SMALLINT, Short.class),
    INTEGER(3, "int32", JDBCType.INTEGER, Integer.class),
    BIGINT(4, "int64", JDBCType.BIGINT, Long.class),

    //UINT8(5, null, JDBCType.OTHER),
    //UINT16(6, null, JDBCType.OTHER),
    //UINT32(7, null, JDBCType.OTHER),
    //UINT64(8, null, JDBCType.OTHER),
    //BINARY(9, "binary", JDBCType.VARBINARY, byte[].class),
    //BITARRAY(10, null, JDBCType.VARBINARY),

    FLOAT(11, "float", JDBCType.REAL, Float.class),
    DOUBLE(12, "double", JDBCType.DOUBLE, Double.class),
    STRING(13, "string", JDBCType.VARCHAR, String.class),
    MISSING(14, "missing", JDBCType.OTHER, Void.class), // don't report as JDBCType.NULL
    BOOLEAN(15, "boolean", JDBCType.BOOLEAN, Boolean.class),
    DATETIME(16, "datetime", JDBCType.TIMESTAMP, java.sql.Timestamp.class),
    DATE(17, "date", JDBCType.DATE, java.sql.Date.class),
    TIME(18, "time", JDBCType.TIME, java.sql.Time.class),
    DURATION(19, "duration", JDBCType.OTHER, String.class),

    //POINT(20, "point", JDBCType.OTHER, Object.class),
    //POINT3D(21, "point3d", JDBCType.OTHER, Object.class),

    ARRAY(22, "array", JDBCType.OTHER, List.class),
    MULTISET(23, "multiset", JDBCType.OTHER, List.class),
    OBJECT(24, "object", JDBCType.OTHER, Map.class),

    //SPARSOBJECT(25, null, null, JDBCType.OTHER),
    //UNION(26, null, JDBCType.OTHER),
    //ENUM(27, null, JDBCType.OTHER),
    //TYPE(28, null, JDBCType.OTHER),

    ANY(29, "any", JDBCType.OTHER, String.class),

    //LINE(30, "line", JDBCType.OTHER, Object.class),
    //POLYGON(31, "polygon", JDBCType.OTHER, Object.class),
    //CIRCLE(32, "circle", JDBCType.OTHER, Object.class),
    //RECTANGLE(33, "rectangle", JDBCType.OTHER, Object.class),
    //INTERVAL(34, "interval", JDBCType.OTHER, Object.class),
    //SYSTEM_NULL(35, null, null, JDBCType.OTHER),

    YEARMONTHDURATION(36, "year-month-duration", JDBCType.OTHER, java.time.Period.class),
    DAYTIMEDURATION(37, "day-time-duration", JDBCType.OTHER, java.time.Duration.class),
    UUID(38, "uuid", JDBCType.OTHER, java.util.UUID.class),

    //SHORTWITHOUTTYPEINFO(40, null, null, JDBCType.OTHER),

    NULL(41, "null", JDBCType.NULL, Void.class);

    //GEOMETRY(42, "geometry", JDBCType.OTHER, Object.class)

    private static final ADBDatatype[] BY_TYPE_TAG;

    private static final Map<String, ADBDatatype> BY_TYPE_NAME;

    private final byte typeTag;

    private final String typeName;

    private final JDBCType jdbcType;

    private final Class<?> javaClass;

    ADBDatatype(int typeTag, String typeName, JDBCType jdbcType, Class<?> javaClass) {
        this.typeTag = (byte) typeTag;
        this.typeName = Objects.requireNonNull(typeName);
        this.jdbcType = Objects.requireNonNull(jdbcType);
        this.javaClass = Objects.requireNonNull(javaClass);
    }

    byte getTypeTag() {
        return typeTag;
    }

    String getTypeName() {
        return typeName;
    }

    JDBCType getJdbcType() {
        return jdbcType;
    }

    Class<?> getJavaClass() {
        return javaClass;
    }

    @Override
    public String toString() {
        return getTypeName();
    }

    boolean isDerived() {
        return this == OBJECT || isList();
    }

    boolean isList() {
        return this == ARRAY || this == MULTISET;
    }

    boolean isNullOrMissing() {
        return this == NULL || this == MISSING;
    }

    static {
        ADBDatatype[] allTypes = ADBDatatype.values();
        ADBDatatype[] byTypeTag = new ADBDatatype[findMaxTypeTag(allTypes) + 1];
        Map<String, ADBDatatype> byTypeName = new HashMap<>();
        for (ADBDatatype t : allTypes) {
            byTypeTag[t.typeTag] = t;
            byTypeName.put(t.typeName, t);
        }
        BY_TYPE_TAG = byTypeTag;
        BY_TYPE_NAME = byTypeName;
    }

    public static ADBDatatype findByTypeTag(byte typeTag) {
        return typeTag >= 0 && typeTag < BY_TYPE_TAG.length ? BY_TYPE_TAG[typeTag] : null;
    }

    public static ADBDatatype findByTypeName(String typeName) {
        return BY_TYPE_NAME.get(typeName);
    }

    private static int findMaxTypeTag(ADBDatatype[] allTypes) {
        int maxTypeTag = 0;
        for (ADBDatatype type : allTypes) {
            if (type.typeTag < 0) {
                throw new IllegalStateException(type.getTypeName());
            }
            maxTypeTag = Math.max(type.typeTag, maxTypeTag);
        }
        return maxTypeTag;
    }

    static String getDerivedRecordName(ADBDatatype type) {
        switch (type) {
            case OBJECT:
                return "Record";
            case ARRAY:
                return "OrderedList";
            case MULTISET:
                return "UnorderedList";
            default:
                throw new IllegalArgumentException(String.valueOf(type));
        }
    }
}
