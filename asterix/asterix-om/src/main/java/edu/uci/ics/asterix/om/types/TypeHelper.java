package edu.uci.ics.asterix.om.types;

import java.util.ArrayList;
import java.util.List;

public class TypeHelper {

    public static boolean canBeNull(IAType t) {
        switch (t.getTypeTag()) {
            case NULL: {
                return true;
            }
            case UNION: {
                AUnionType ut = (AUnionType) t;
                for (IAType t2 : ut.getUnionList()) {
                    if (canBeNull(t2)) {
                        return true;
                    }
                }
                return false;
            }
            default: {
                return false;
            }
        }
    }

    public static IAType getNonOptionalType(IAType t) {
        if (t.getTypeTag() != ATypeTag.UNION) {
            return t;
        }
        AUnionType ut = (AUnionType) t;
        List<IAType> x = new ArrayList<IAType>();
        for (IAType t1 : ut.getUnionList()) {
            IAType y = getNonOptionalType(t1);
            if (y != BuiltinType.ANULL) {
                x.add(y);
            }
        }
        if (x.isEmpty()) {
            return BuiltinType.ANULL;
        }
        if (x.size() == 1) {
            return x.get(0);
        }
        return new AUnionType(x, null);
    }

    public static boolean isClosed(IAType t) {
        switch (t.getTypeTag()) {
            case ANY: {
                return false;
            }
            case UNION: {
                AUnionType ut = (AUnionType) t;
                for (IAType t1 : ut.getUnionList()) {
                    if (!isClosed(t1)) {
                        return false;
                    }
                }
                return true;
            }
            default: {
                return true;
            }
        }
    }

}
