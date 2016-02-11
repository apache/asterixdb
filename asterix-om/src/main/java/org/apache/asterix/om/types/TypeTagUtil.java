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

import org.apache.asterix.common.exceptions.AsterixException;

public class TypeTagUtil {

    public static IAType getBuiltinTypeByTag(ATypeTag typeTag) throws AsterixException {
        switch (typeTag) {
            case INT8:
                return BuiltinType.AINT8;
            case INT16:
                return BuiltinType.AINT16;
            case INT32:
                return BuiltinType.AINT32;
            case INT64:
                return BuiltinType.AINT64;
            case BINARY:
                return BuiltinType.ABINARY;
            case BITARRAY:
                return BuiltinType.ABITARRAY;
            case FLOAT:
                return BuiltinType.AFLOAT;
            case DOUBLE:
                return BuiltinType.ADOUBLE;
            case STRING:
                return BuiltinType.ASTRING;
            case NULL:
                return BuiltinType.ANULL;
            case BOOLEAN:
                return BuiltinType.ABOOLEAN;
            case DATETIME:
                return BuiltinType.ADATETIME;
            case DATE:
                return BuiltinType.ADATE;
            case TIME:
                return BuiltinType.ATIME;
            case DURATION:
                return BuiltinType.ADURATION;
            case POINT:
                return BuiltinType.APOINT;
            case POINT3D:
                return BuiltinType.APOINT3D;
            case TYPE:
                return BuiltinType.ASTERIX_TYPE;
            case ANY:
                return BuiltinType.ANY;
            case LINE:
                return BuiltinType.ALINE;
            case POLYGON:
                return BuiltinType.APOLYGON;
            case CIRCLE:
                return BuiltinType.ACIRCLE;
            case RECTANGLE:
                return BuiltinType.ARECTANGLE;
            case INTERVAL:
                return BuiltinType.AINTERVAL;
            case YEARMONTHDURATION:
                return BuiltinType.AYEARMONTHDURATION;
            case DAYTIMEDURATION:
                return BuiltinType.ADAYTIMEDURATION;
            case UUID:
                return BuiltinType.AUUID;
            default:
                throw new AsterixException("Typetag " + typeTag + " is not a built-in type");
        }
    }
}
