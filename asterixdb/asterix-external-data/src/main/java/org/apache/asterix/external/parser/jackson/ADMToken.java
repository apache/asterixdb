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
package org.apache.asterix.external.parser.jackson;

import org.apache.asterix.om.types.ATypeTag;

public enum ADMToken {
    /*
     ****************************************************
     * JSON format values
     ****************************************************
     */

    NULL(ATypeTag.NULL),
    FALSE(ATypeTag.BOOLEAN),
    TRUE(ATypeTag.BOOLEAN),
    INT(ATypeTag.BIGINT), //Default type of integers
    DOUBLE(ATypeTag.DOUBLE), //Default type of floating-points
    STRING(ATypeTag.STRING),
    OBJECT_START(ATypeTag.OBJECT),
    OBJECT_END,
    ARRAY_START(ATypeTag.ARRAY),
    ARRAY_END,

    //field name
    FIELD_NAME,

    /*
     ****************************************************
     * ADM - Atomic value constructors
     ****************************************************
     */

    //numeric constructors
    TINYINT_ADM(ATypeTag.TINYINT),
    INT_ADM(ATypeTag.INTEGER),
    BIGINT_ADM(ATypeTag.BIGINT),
    FLOAT_ADM(ATypeTag.FLOAT),
    DOUBLE_ADM(ATypeTag.DOUBLE),

    //spatial
    POINT_ADM(ATypeTag.POINT),
    LINE_ADM(ATypeTag.LINE),
    CIRCLE_ADM(ATypeTag.CIRCLE),
    RECTANGLE_ADM(ATypeTag.RECTANGLE),
    POLYGON_ADM(ATypeTag.POLYGON),

    //temporal
    TIME_ADM(ATypeTag.TIME),
    DATE_ADM(ATypeTag.DATE),
    DATETIME_ADM(ATypeTag.DATETIME),
    DURATION_ADM(ATypeTag.DURATION),
    YEAR_MONTH_DURATION_ADM(ATypeTag.YEARMONTHDURATION),
    DAY_TIME_DURATION_ADM(ATypeTag.DAYTIMEDURATION),
    INTERVAL_ADM(ATypeTag.INTERVAL),

    //other
    UUID_ADM(ATypeTag.UUID),
    BINARY_ADM(ATypeTag.BINARY),

    /*
     ****************************************************
     * Parser control tokens
     ****************************************************
     */

    PROCEED,
    SKIP,
    EOF;

    private final ATypeTag tokenMappedType;

    private ADMToken() {
        tokenMappedType = null;
    }

    private ADMToken(ATypeTag typeTag) {
        this.tokenMappedType = typeTag;
    }

    public ATypeTag getTypeTag() {
        return tokenMappedType;
    }
}
