package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericDivideDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericDivideDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.NUMERIC_DIVIDE;
    }

    @Override
    protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException {
        if (rhs == 0)
            throw new HyracksDataException("Divide by Zero.");
        return lhs / rhs;
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) {
        return lhs / rhs;
    }
}
