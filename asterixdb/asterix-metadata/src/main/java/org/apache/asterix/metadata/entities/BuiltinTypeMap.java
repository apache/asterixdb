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

package org.apache.asterix.metadata.entities;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * Maps from a string representation of an Asterix type to an Asterix type.
 */
public class BuiltinTypeMap {

    private static final Map<String, BuiltinType> _builtinTypeMap = new HashMap<>();

    static {
        // Builtin types with deprecated names.
        _builtinTypeMap.put("int8", BuiltinType.AINT8);
        _builtinTypeMap.put("int16", BuiltinType.AINT16);
        _builtinTypeMap.put("int32", BuiltinType.AINT32);
        _builtinTypeMap.put("int64", BuiltinType.AINT64);
        _builtinTypeMap.put("year-month-duration", BuiltinType.AYEARMONTHDURATION);
        _builtinTypeMap.put("day-time-duration", BuiltinType.ADAYTIMEDURATION);

        // Builtin types.
        _builtinTypeMap.put("boolean", BuiltinType.ABOOLEAN);
        _builtinTypeMap.put("tinyint", BuiltinType.AINT8);
        _builtinTypeMap.put("smallint", BuiltinType.AINT16);
        _builtinTypeMap.put("integer", BuiltinType.AINT32);
        _builtinTypeMap.put("int", BuiltinType.AINT32);
        _builtinTypeMap.put("bigint", BuiltinType.AINT64);
        _builtinTypeMap.put("float", BuiltinType.AFLOAT);
        _builtinTypeMap.put("double", BuiltinType.ADOUBLE);
        _builtinTypeMap.put("double precision", BuiltinType.ADOUBLE);
        _builtinTypeMap.put("string", BuiltinType.ASTRING);
        _builtinTypeMap.put("binary", BuiltinType.ABINARY);
        _builtinTypeMap.put("date", BuiltinType.ADATE);
        _builtinTypeMap.put("time", BuiltinType.ATIME);
        _builtinTypeMap.put("datetime", BuiltinType.ADATETIME);
        _builtinTypeMap.put("timestamp", BuiltinType.ADATETIME);
        _builtinTypeMap.put("duration", BuiltinType.ADURATION);
        _builtinTypeMap.put("year_month_duration", BuiltinType.AYEARMONTHDURATION);
        _builtinTypeMap.put("day_time_duration", BuiltinType.ADAYTIMEDURATION);
        _builtinTypeMap.put("interval", BuiltinType.AINTERVAL);
        _builtinTypeMap.put("point", BuiltinType.APOINT);
        _builtinTypeMap.put("point3d", BuiltinType.APOINT3D);
        _builtinTypeMap.put("line", BuiltinType.ALINE);
        _builtinTypeMap.put("polygon", BuiltinType.APOLYGON);
        _builtinTypeMap.put("circle", BuiltinType.ACIRCLE);
        _builtinTypeMap.put("rectangle", BuiltinType.ARECTANGLE);
        _builtinTypeMap.put("missing", BuiltinType.AMISSING);
        _builtinTypeMap.put("null", BuiltinType.ANULL);
        _builtinTypeMap.put("uuid", BuiltinType.AUUID);
        _builtinTypeMap.put("shortwithouttypeinfo", BuiltinType.SHORTWITHOUTTYPEINFO);
        _builtinTypeMap.put("geometry", BuiltinType.AGEOMETRY);
    }

    private BuiltinTypeMap() {

    }

    public static IAType getBuiltinType(String typeName) {
        return _builtinTypeMap.get(typeName.toLowerCase());
    }

    public static Set<IAType> getAllBuiltinTypes() {
        return new HashSet<>(_builtinTypeMap.values());
    }

    public static IAType getTypeFromTypeName(MetadataNode metadataNode, TxnId txnId, String dataverseName,
            String typeName, boolean optional) throws AlgebricksException {
        IAType type = _builtinTypeMap.get(typeName);
        if (type == null) {
            try {
                Datatype dt = metadataNode.getDatatype(txnId, dataverseName, typeName);
                type = dt.getDatatype();
            } catch (RemoteException e) {
                throw new MetadataException(e);
            }
        }
        if (optional) {
            type = AUnionType.createUnknownableType(type);
        }
        return type;
    }
}
