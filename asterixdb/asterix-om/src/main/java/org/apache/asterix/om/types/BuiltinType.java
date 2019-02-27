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

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import org.apache.asterix.om.base.IAObject;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class BuiltinType implements IAType {

    private static final long serialVersionUID = 1L;
    private static final String TAG_FIELD = "tag";

    public abstract static class LowerCaseConstructorType extends BuiltinType {
        private static final long serialVersionUID = 1L;

        @Override
        public String getConstructor() {
            return getTypeTag().toString().toLowerCase();
        }
    }

    /** the type of all types */
    public static final BuiltinType ALL_TYPE = new BuiltinType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.TYPE;
        }

        @Override
        public String getDisplayName() {
            return "AllType";
        }

        @Override
        public String getTypeName() {
            return "ALL_TYPE";
        }

        @Override
        public String getConstructor() {
            return null;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ALL_TYPE");
            return type;
        }
    };

    public static final BuiltinType AINT8 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt8";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.TINYINT;
        }

        @Override
        public String getTypeName() {
            return "int8";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AInt8");
            return type;
        }
    };

    public static final BuiltinType AINT16 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt16";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.SMALLINT;
        }

        @Override
        public String getTypeName() {
            return "int16";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AInt16");
            return type;
        }
    };

    public static final BuiltinType AINT32 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt32";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INTEGER;
        }

        @Override
        public String getTypeName() {
            return "int32";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AInt32");
            return type;
        }
    };

    public static final BuiltinType AINT64 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.BIGINT;
        }

        @Override
        public String getDisplayName() {
            return "AInt64";
        }

        @Override
        public String getTypeName() {
            return "int64";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AInt64");
            return type;
        }
    };

    public static final BuiltinType ABINARY = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ABinary";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.BINARY;
        }

        @Override
        public String getTypeName() {
            return "binary";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ABinary");
            return type;
        }
    };

    public static final BuiltinType AFLOAT = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AFloat";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.FLOAT;
        }

        @Override
        public String getTypeName() {
            return "float";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AFloat");
            return type;
        }
    };

    public static final BuiltinType ADOUBLE = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ADouble";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DOUBLE;
        }

        @Override
        public String getTypeName() {
            return "double";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ADouble");
            return type;
        }
    };

    public static final BuiltinType ASTRING = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AString";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.STRING;
        }

        @Override
        public String getTypeName() {
            return "string";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AString");
            return type;
        }
    };

    public static final BuiltinType AMISSING = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "Missing";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.MISSING;
        }

        @Override
        public String getTypeName() {
            return "missing";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AMISSING");
            return type;
        }
    };

    public static final BuiltinType ANULL = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "Null";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.NULL;
        }

        @Override
        public String getTypeName() {
            return "null";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ANULL");
            return type;
        }
    };

    public static final BuiltinType ABOOLEAN = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ABoolean";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.BOOLEAN;
        }

        @Override
        public String getTypeName() {
            return "boolean";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ABoolean");
            return type;
        }
    };

    public static final BuiltinType ATIME = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ATime";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.TIME;
        }

        @Override
        public String getTypeName() {
            return "time";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ATime");
            return type;
        }
    };

    public static final BuiltinType ADATE = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ADate";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DATE;
        }

        @Override
        public String getTypeName() {
            return "date";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ADate");
            return type;
        }
    };

    public static final BuiltinType ADATETIME = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ADateTime";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DATETIME;
        }

        @Override
        public String getTypeName() {
            return "datetime";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ADateTime");
            return type;
        }
    };

    public static final BuiltinType ADURATION = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ADuration";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DURATION;
        }

        @Override
        public String getTypeName() {
            return "duration";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ADuration");
            return type;
        }
    };

    public static final BuiltinType AYEARMONTHDURATION = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AYearMonthDuration";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.YEARMONTHDURATION;
        }

        @Override
        public String getTypeName() {
            return "year-month-duration";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "AYearMonthDuration");
            return type;
        }
    };

    public static final BuiltinType ADAYTIMEDURATION = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ADayTimeDuration";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DAYTIMEDURATION;
        }

        @Override
        public String getTypeName() {
            return "day-time-duration";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ADayTimeDuration");
            return type;
        }
    };

    public static final BuiltinType AINTERVAL = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInterval";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INTERVAL;
        }

        @Override
        public String getTypeName() {
            return "interval";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            return null;
        }
    };

    public static final BuiltinType APOINT = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POINT;
        }

        @Override
        public String getDisplayName() {
            return "APoint";
        }

        @Override
        public String getTypeName() {
            return "point";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "APoint");
            return type;
        }
    };

    public static final BuiltinType APOINT3D = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POINT3D;
        }

        @Override
        public String getDisplayName() {
            return "APoint3D";
        }

        @Override
        public String getTypeName() {
            return "point3d";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "APoint3D");
            return type;
        }
    };

    public static final BuiltinType ALINE = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ALINE";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.LINE;
        }

        @Override
        public String getTypeName() {
            return "line";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ALINE");
            return type;
        }
    };

    public static final BuiltinType APOLYGON = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "APOLYGON";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POLYGON;
        }

        @Override
        public String getTypeName() {
            return "polygon";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "APOLYGON");
            return type;
        }
    };

    public static final BuiltinType AGEOMETRY = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AGEOMETRY";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.GEOMETRY;
        }

        @Override
        public String getTypeName() {
            return "geometry";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectNode type = new ObjectMapper().createObjectNode();
            type.put("type", "AGEOMETRY");
            return type;
        }
    };

    public static final BuiltinType ACIRCLE = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ACIRCLE";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.CIRCLE;
        }

        @Override
        public String getTypeName() {
            return "circle";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ACIRCLE");
            return type;
        }
    };

    public static final BuiltinType ARECTANGLE = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "ARECTANGLE";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.RECTANGLE;
        }

        @Override
        public String getTypeName() {
            return "rectangle";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ARECTANGLE");
            return type;
        }
    };

    public static final IAType ABITARRAY = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.BITARRAY;
        }

        @Override
        public String getDisplayName() {
            return "ABitArray";
        }

        @Override
        public String getTypeName() {
            return "abitarray";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ABitArray");
            return type;
        }
    };

    public static final BuiltinType AUUID = new LowerCaseConstructorType() {
        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.UUID;
        }

        @Override
        public String getDisplayName() {
            return "UUID";
        }

        @Override
        public String getTypeName() {
            return "uuid";
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", getDisplayName());
            return type;
        }
    };

    public static final BuiltinType ANY = new BuiltinType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.ANY;
        }

        @Override
        public String getTypeName() {
            return "any";
        }

        @Override
        public String getDisplayName() {
            return "ANY";
        }

        @Override
        public String getConstructor() {
            return null;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "ANY");
            return type;
        }
    };

    public static final BuiltinType SHORTWITHOUTTYPEINFO = new BuiltinType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.SHORTWITHOUTTYPEINFO;
        }

        @Override
        public String getTypeName() {
            return "shortwithouttypeinfo";
        }

        @Override
        public String getDisplayName() {
            return "SHORTWITHOUTTYPEINFO";
        }

        @Override
        public String getConstructor() {
            return null;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) {
            return convertToJson(registry, getTypeTag().serialize(), serialVersionUID);
        }

        @Override
        public ObjectNode toJSON() {
            ObjectMapper om = new ObjectMapper();
            ObjectNode type = om.createObjectNode();
            type.put("type", "SHORTWITHOUTTYPEINFO");
            return type;
        }
    };

    public abstract String getConstructor();

    @Override
    public IAType getType() {
        return ALL_TYPE;
    }

    @Override
    public String toString() {
        return getTypeTag().toString();
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BuiltinType)) {
            return false;
        }
        return ((BuiltinType) obj).getTypeTag().equals(getTypeTag());
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof IAObject && deepEqual((IAObject) object);
    }

    @Override
    public int hashCode() {
        return getTypeTag().hashCode();
    }

    @Override
    public int hash() {
        return getType().getTypeTag().serialize();
    }

    private static JsonNode convertToJson(IPersistedResourceRegistry registry, short tag, long version) {
        ObjectNode jsonNode = registry.getClassIdentifier(BuiltinType.class, version);
        jsonNode.put(TAG_FIELD, tag);
        return jsonNode;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        byte tag = (byte) json.get(TAG_FIELD).shortValue();
        ATypeTag typeTag = VALUE_TYPE_MAPPING[tag];
        switch (typeTag) {
            case TYPE:
                return ALL_TYPE;
            case TINYINT:
                return AINT8;
            case SMALLINT:
                return AINT16;
            case INTEGER:
                return AINT32;
            case BIGINT:
                return AINT64;
            case FLOAT:
                return AFLOAT;
            case DOUBLE:
                return ADOUBLE;
            case STRING:
                return ASTRING;
            case BINARY:
                return ABINARY;
            case MISSING:
                return AMISSING;
            case NULL:
                return ANULL;
            case BOOLEAN:
                return ABOOLEAN;
            case TIME:
                return ATIME;
            case DATE:
                return ADATE;
            case DATETIME:
                return ADATETIME;
            case DURATION:
                return ADURATION;
            case YEARMONTHDURATION:
                return AYEARMONTHDURATION;
            case DAYTIMEDURATION:
                return ADAYTIMEDURATION;
            case INTERVAL:
                return AINTERVAL;
            case POINT:
                return APOINT;
            case POINT3D:
                return APOINT3D;
            case LINE:
                return ALINE;
            case POLYGON:
                return APOLYGON;
            case GEOMETRY:
                return AGEOMETRY;
            case CIRCLE:
                return ACIRCLE;
            case RECTANGLE:
                return ARECTANGLE;
            case BITARRAY:
                return ABITARRAY;
            case UUID:
                return AUUID;
            case ANY:
                return ANY;
            case SHORTWITHOUTTYPEINFO:
                return SHORTWITHOUTTYPEINFO;
            default:
                throw new UnsupportedOperationException(typeTag.toString());
        }
    }
}
