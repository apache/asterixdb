package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class UnaryBooleanOrNullFunctionTypeComputer implements IResultTypeComputer {

    public static final UnaryBooleanOrNullFunctionTypeComputer INSTANCE = new UnaryBooleanOrNullFunctionTypeComputer();

    private UnaryBooleanOrNullFunctionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        IAType t0;
        try {
            t0 = (IAType) env.getType(arg0);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t0.getTypeTag() == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }
        if (TypeHelper.canBeNull(t0)) {
            List<IAType> unionList = new ArrayList<IAType>();
            unionList.add(BuiltinType.ANULL);
            unionList.add(BuiltinType.ABOOLEAN);
            return new AUnionType(unionList, "OptionalBoolean");
        }
        return BuiltinType.ABOOLEAN;
    }

}
