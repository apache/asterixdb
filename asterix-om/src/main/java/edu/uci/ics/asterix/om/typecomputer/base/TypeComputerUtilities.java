package edu.uci.ics.asterix.om.typecomputer.base;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class TypeComputerUtilities {

    public static boolean setRequiredAndInputTypes(AbstractFunctionCallExpression expr, IAType requiredRecordType,
            IAType inputRecordType) {
        boolean changed = false;
        Object opaqueParameter = expr.getOpaqueParameters();
        if (opaqueParameter == null) {
            Object[] opaqueParameters = new Object[2];
            opaqueParameters[0] = requiredRecordType;
            opaqueParameters[1] = inputRecordType;
            expr.setOpaqueParameters(opaqueParameters);
            changed = true;
        }
        return changed;
    }

    public static IAType getRequiredType(AbstractFunctionCallExpression expr) {
        Object[] type = expr.getOpaqueParameters();
        if (type != null) {
            IAType recordType = (IAType) type[0];
            return recordType;
        } else
            return null;
    }

    public static IAType getInputType(AbstractFunctionCallExpression expr) {
        Object[] type = expr.getOpaqueParameters();
        if (type != null) {
            IAType recordType = (IAType) type[1];
            return recordType;
        } else
            return null;
    }
}
