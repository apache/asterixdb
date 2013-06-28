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

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public abstract class BuiltinType implements IAType {

    private static final long serialVersionUID = 1L;

    public abstract static class LowerCaseConstructorType extends BuiltinType {
        private static final long serialVersionUID = 1L;

        @Override
        public String getConstructor() {
            return getTypeTag().toString().toLowerCase();
        }
    }

    /** the type of all types */
    public final static BuiltinType ASTERIX_TYPE = new BuiltinType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.TYPE;
        }

        @Override
        public String getDisplayName() {
            return "AsterixType";
        }

        @Override
        public String getTypeName() {
            return "atype";
        }

        @Override
        public String getConstructor() {
            return null;
        }

        @Override
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AsterixType");
            return type;
        }
    };

    public final static BuiltinType AINT8 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt8";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INT8;
        }

        @Override
        public String getTypeName() {
            return "int8";
        }

        @Override
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AInt8");
            return type;
        }
    };

    public final static BuiltinType AINT16 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt16";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INT16;
        }

        @Override
        public String getTypeName() {
            return "int16";
        }

        @Override
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AInt16");
            return type;
        }
    };

    public final static BuiltinType AINT32 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public String getDisplayName() {
            return "AInt32";
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INT32;
        }

        @Override
        public String getTypeName() {
            return "int32";
        }

        @Override
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AInt32");
            return type;
        }
    };

    public static final BuiltinType AINT64 = new LowerCaseConstructorType() {

        private static final long serialVersionUID = 1L;

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INT64;
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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AInt64");
            return type;
        }
    };

    public final static BuiltinType ABINARY = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ABinary");
            return type;
        }
    };

    public final static BuiltinType AFLOAT = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AFloat");
            return type;
        }
    };

    public final static BuiltinType ADOUBLE = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ADouble");
            return type;
        }
    };

    public final static BuiltinType ASTRING = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AString");
            return type;
        }
    };

    public final static BuiltinType ANULL = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "Null");
            return type;
        }
    };

    public final static BuiltinType ABOOLEAN = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ABoolean");
            return type;
        }
    };

    public final static BuiltinType ATIME = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ATime");
            return type;
        }
    };

    public final static BuiltinType ADATE = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ADate");
            return type;
        }
    };

    public final static BuiltinType ADATETIME = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ADateTime");
            return type;
        }
    };

    public final static BuiltinType ADURATION = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ADuration");
            return type;
        }
    };

    public final static BuiltinType AYEARMONTHDURATION = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "AYearMonthDuration");
            return type;
        }
    };

    public final static BuiltinType ADAYTIMEDURATION = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ADayTimeDuration");
            return type;
        }
    };

    public final static BuiltinType AINTERVAL = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            return null;
        }
    };

    public final static BuiltinType APOINT = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "APoint");
            return type;
        }
    };

    public final static BuiltinType APOINT3D = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "APoint3D");
            return type;
        }
    };

    public final static BuiltinType ALINE = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ALINE");
            return type;
        }
    };

    public final static BuiltinType APOLYGON = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "APOLYGON");
            return type;
        }
    };

    public final static BuiltinType ACIRCLE = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ACIRCLE");
            return type;
        }
    };

    public final static BuiltinType ARECTANGLE = new LowerCaseConstructorType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ABitArray");
            return type;
        }
    };

    public static final IAType ANY = new BuiltinType() {

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
        public JSONObject toJSON() throws JSONException {
            JSONObject type = new JSONObject();
            type.put("type", "ANY");
            return type;
        }
    };

    public abstract String getConstructor();

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public IAType getType() {
        return ASTERIX_TYPE;
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
        return this.deepEqual((IAObject) object);
    }

    @Override
    public int hashCode() {
        return getTypeTag().hashCode();
    }

    @Override
    public int hash() {
        return getType().getTypeTag().serialize();
    }

    public static BuiltinType builtinTypeFromString(String str) throws AsterixException {
        if (str.equals(BuiltinType.AINT32.getTypeName())) {
            return BuiltinType.AINT32;
        } else if (str.equals(BuiltinType.ASTRING.getTypeName())) {
            return BuiltinType.ASTRING;
        } else if (str.equals(BuiltinType.ADOUBLE.getTypeName())) {
            return BuiltinType.ADOUBLE;
        } else if (str.equals(BuiltinType.AFLOAT.getTypeName())) {
            return BuiltinType.AFLOAT;
        }
        throw new AsterixException("No string translation for type: " + str + " .");
    }

    public static ATypeTag builtinTypeTagFromString(String str) throws AsterixException {
        if (str.equals("int32")) {
            return ATypeTag.INT32;
        } else if (str.equals("string")) {
            return ATypeTag.STRING;
        } else if (str.equals("double")) {
            return ATypeTag.DOUBLE;
        } else if (str.equals("float")) {
            return ATypeTag.FLOAT;
        }
        throw new AsterixException("No string translation for type: " + str + " .");
    }

}
