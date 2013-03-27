package edu.uci.ics.asterix.runtime.evaluators.common;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AsterixFunctionInputTypeException extends AlgebricksException {

    private static final long serialVersionUID = 1L;

    public AsterixFunctionInputTypeException(FunctionIdentifier funcID, String message) {
        super(funcID.getName() + ": " + message);
    }

    public AsterixFunctionInputTypeException(FunctionIdentifier funcID, String message, ATypeTag... inputTypes) {
        super(getErrorMsg(funcID, message, inputTypes));
    }

    public static String getErrorMsg(FunctionIdentifier funcID, String message, ATypeTag[] inputTypes) {
        StringBuilder sbder = new StringBuilder();
        sbder.append(funcID.getName()).append(" (").append(inputTypes[0].name());
        for (ATypeTag tTag : inputTypes) {
            sbder.append(", ").append(tTag.name());
        }
        sbder.append(": ").append(message);
        return sbder.toString();
    }

}
