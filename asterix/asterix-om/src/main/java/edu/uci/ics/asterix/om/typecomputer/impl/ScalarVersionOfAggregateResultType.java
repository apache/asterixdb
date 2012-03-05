package edu.uci.ics.asterix.om.typecomputer.impl;


import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ScalarVersionOfAggregateResultType implements IResultTypeComputer {

    public static final ScalarVersionOfAggregateResultType INSTANCE = new ScalarVersionOfAggregateResultType();

    private ScalarVersionOfAggregateResultType() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        IAType t1 = (IAType) env.getType(arg1);
        IAType nonOpt = TypeHelper.getNonOptionalType(t1);
        ATypeTag tag1 = nonOpt.getTypeTag();
        if (tag1 != ATypeTag.ORDEREDLIST && tag1 != ATypeTag.UNORDEREDLIST) {
            throw new AlgebricksException("Type of argument in " + expression
                    + " should be a collection type instead of " + t1);
        }
        AbstractCollectionType act = (AbstractCollectionType) nonOpt;
        IAType t = act.getItemType();
        if (TypeHelper.canBeNull(t)) {
            return t;
        } else {
            return AUnionType.createNullableType(t);
        }
    }

}
