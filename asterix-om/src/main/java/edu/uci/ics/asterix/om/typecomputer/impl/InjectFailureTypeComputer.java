package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class InjectFailureTypeComputer implements IResultTypeComputer {

    private static final String errMsg1 = "inject-failure should have at least 2 parameters ";
    private static final String errMsg2 = "failure condition expression should have the return type Boolean";

    public static IResultTypeComputer INSTANCE = new InjectFailureTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 2)
            throw new AlgebricksException(errMsg1);

        IAType t0 = (IAType) env.getType(fce.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(fce.getArguments().get(0).getValue());
        ATypeTag tag1 = t1.getTypeTag();
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1))
            tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();

        if (tag1 != ATypeTag.BOOLEAN)
            throw new AlgebricksException(errMsg2);

        return t0;
    }

}
