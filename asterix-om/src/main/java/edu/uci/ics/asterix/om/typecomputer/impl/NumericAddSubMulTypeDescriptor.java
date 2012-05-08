package edu.uci.ics.asterix.om.typecomputer.impl;


import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NumericAddSubMulTypeDescriptor implements IResultTypeComputer {

    private static final String errMsg = "Arithmetic operations are only implemented for AINT32 and ADOUBLE.";

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(1).getValue();
        IAType t1;
        IAType t2;
        try {
            t1 = (IAType) env.getType(arg1);
            t2 = (IAType) env.getType(arg2);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t1 == null || t2 == null) {
            return null;
        }
        switch (t1.getTypeTag()) {
            case INT32: {
                switch (t2.getTypeTag()) {
                    case INT32: {
                        return BuiltinType.AINT32;
                    }
                    case DOUBLE: {
                        return BuiltinType.ADOUBLE;
                    }
                    default: {
                        throw new NotImplementedException(errMsg);
                    }
                }
            }
            case DOUBLE: {
                switch (t2.getTypeTag()) {
                    case INT32:
                    case DOUBLE: {
                        return BuiltinType.ADOUBLE;
                    }
                    default: {
                        throw new NotImplementedException(errMsg);
                    }
                }
            }
            default: {
                throw new NotImplementedException(errMsg);
            }
        }
    }

}
