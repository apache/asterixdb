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

package org.apache.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

class TypeCompatibilityChecker {
    private final List<IAType> possibleTypes;
    private boolean nullEncountered;

    public TypeCompatibilityChecker() {
        possibleTypes = new ArrayList<IAType>();
        nullEncountered = false;
    }

    public void reset() {
        possibleTypes.clear();
        nullEncountered = false;
    }

    public void addPossibleType(IAType type) {
        if (type.getTypeTag() == ATypeTag.UNION) {
            List<IAType> typeList = ((AUnionType) type).getUnionList();
            for (IAType t : typeList) {
                if (t.getTypeTag() != ATypeTag.NULL) {
                    //CONCAT_NON_NULL cannot return null because it's only used for if-else construct
                    if (!possibleTypes.contains(t))
                        possibleTypes.add(t);
                } else {
                    nullEncountered = true;
                }
            }
        } else {
            if (type.getTypeTag() != ATypeTag.NULL) {
                if (!possibleTypes.contains(type)) {
                    possibleTypes.add(type);
                }
            } else {
                nullEncountered = true;
            }
        }
    }

    public IAType getCompatibleType() {
        switch (possibleTypes.size()) {
            case 0:
                return BuiltinType.ANULL;
            case 1:
                if (nullEncountered) {
                    return AUnionType.createNullableType(possibleTypes.get(0));
                } else {
                    return possibleTypes.get(0);
                }
        }
        return null;
    }
}