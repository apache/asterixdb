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
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.visitors.IOMVisitor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AUnionType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    public static final int OPTIONAL_TYPE_INDEX_IN_UNION_LIST = 1;
    private final List<IAType> unionList;

    public AUnionType(List<IAType> unionList, String typeName) {
        super(typeName);
        this.unionList = unionList;
    }

    public List<IAType> getUnionList() {
        return unionList;
    }

    public void setTypeAtIndex(IAType type, int index) {
        unionList.set(index, type);
    }

    public boolean isNullableType() {
        return unionList.size() == 2 && unionList.get(0).equals(BuiltinType.ANULL);
    }

    public IAType getNullableType() {
        return unionList.get(AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
    }

    @Override
    public String getDisplayName() {
        return "AUnion";
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNION;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNION(");
        Iterator<IAType> iter = unionList.iterator();
        if (iter.hasNext()) {
            IAType t0 = iter.next();
            sb.append(t0.toString());
            while (iter.hasNext()) {
                sb.append(", " + iter.next());
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    public static AUnionType createNullableType(IAType type, String typeName) {
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        unionList.add(type);
        return new AUnionType(unionList, typeName);
    }

    public static AUnionType createNullableType(IAType t) {
        String s = t != null ? t.getTypeName() : null;
        return createNullableType(t, s == null ? null : s + "?");
    }

    @Override
    public void generateNestedDerivedTypeNames() {
        if (isNullableType()) {
            IAType nullableType = getNullableType();
            if (nullableType.getTypeTag().isDerivedType() && nullableType.getTypeName() == null) {
                AbstractComplexType derivedType = (AbstractComplexType) nullableType;
                derivedType.setTypeName(getTypeName());
                derivedType.generateNestedDerivedTypeNames();
            }

        }
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AUnionType)) {
            return false;
        }
        AUnionType ut = (AUnionType) obj;
        if (ut.getUnionList().size() != unionList.size()) {
            return false;
        }
        for (int i = 0; i < unionList.size(); i++) {
            if (!unionList.get(i).deepEqual(ut.getUnionList().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        int h = 0;
        for (IAType t : unionList) {
            h += 31 * h + t.hash();
        }
        return h;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject type = new JSONObject();
        type.put("type", AUnionType.class.getName());

        JSONArray fields = new JSONArray();
        Iterator<IAType> iter = unionList.iterator();
        if (iter.hasNext()) {
            IAType t0 = iter.next();
            fields.put(t0.toJSON());
            while (iter.hasNext()) {
                fields.put(iter.next().toJSON());
            }
        }

        type.put("fields", fields);
        return type;
    }
}
