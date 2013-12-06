package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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