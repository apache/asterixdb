package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedSumTypeComputer implements IResultTypeComputer {

    private static final String errMsg = "Sum aggregator is not implemented for ";

    public static final NonTaggedSumTypeComputer INSTANCE = new NonTaggedSumTypeComputer();

    private NonTaggedSumTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        IAType t1 = (IAType) env.getType(arg1);
        if (t1 == null) {
            return null;
        }

        ATypeTag tag1;
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1)) {
            tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        } else {
            tag1 = t1.getTypeTag();
        }

        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);

        switch (tag1) {
            case DOUBLE:
                unionList.add(BuiltinType.ADOUBLE);
                break;
            case FLOAT:
                unionList.add(BuiltinType.AFLOAT);
                break;
            case INT64:
                unionList.add(BuiltinType.AINT64);
                break;
            case INT32:
                unionList.add(BuiltinType.AINT32);
                break;
            case INT16:
                unionList.add(BuiltinType.AINT16);
                break;
            case INT8:
                unionList.add(BuiltinType.AINT8);
                break;
            case ANY:
                return BuiltinType.ANY;
            default: {
                throw new NotImplementedException(errMsg + tag1);
            }
        }
        return new AUnionType(unionList, "SumResult");
    }
}
