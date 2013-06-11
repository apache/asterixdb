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

package edu.uci.ics.asterix.metadata.entities;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.om.types.BuiltinType;

/**
 * Maps from a string representation of an Asterix type to an Asterix type.
 */
public class AsterixBuiltinTypeMap {

    private static final Map<String, BuiltinType> _builtinTypeMap = new HashMap<String, BuiltinType>();

    static {
        _builtinTypeMap.put("int8", BuiltinType.AINT8);
        _builtinTypeMap.put("int16", BuiltinType.AINT16);
        _builtinTypeMap.put("int32", BuiltinType.AINT32);
        _builtinTypeMap.put("int64", BuiltinType.AINT64);
        _builtinTypeMap.put("boolean", BuiltinType.ABOOLEAN);
        _builtinTypeMap.put("float", BuiltinType.AFLOAT);
        _builtinTypeMap.put("double", BuiltinType.ADOUBLE);
        _builtinTypeMap.put("string", BuiltinType.ASTRING);
        _builtinTypeMap.put("date", BuiltinType.ADATE);
        _builtinTypeMap.put("time", BuiltinType.ATIME);
        _builtinTypeMap.put("datetime", BuiltinType.ADATETIME);
        _builtinTypeMap.put("duration", BuiltinType.ADURATION);
        _builtinTypeMap.put("year-month-duration", BuiltinType.AYEARMONTHDURATION);
        _builtinTypeMap.put("day-time-duration", BuiltinType.ADAYTIMEDURATION);
        _builtinTypeMap.put("interval", BuiltinType.AINTERVAL);
        _builtinTypeMap.put("point", BuiltinType.APOINT);
        _builtinTypeMap.put("point3d", BuiltinType.APOINT3D);
        _builtinTypeMap.put("line", BuiltinType.ALINE);
        _builtinTypeMap.put("polygon", BuiltinType.APOLYGON);
        _builtinTypeMap.put("circle", BuiltinType.ACIRCLE);
        _builtinTypeMap.put("rectangle", BuiltinType.ARECTANGLE);
        _builtinTypeMap.put("null", BuiltinType.ANULL);
    }

    public static Map<String, BuiltinType> getBuiltinTypes() {
        return _builtinTypeMap;
    }
}
