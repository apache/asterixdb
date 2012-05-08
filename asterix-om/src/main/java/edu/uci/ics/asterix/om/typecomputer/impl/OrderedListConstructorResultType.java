package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class OrderedListConstructorResultType implements IResultTypeComputer {

    public static final OrderedListConstructorResultType INSTANCE = new OrderedListConstructorResultType();

    @Override
    public AOrderedListType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        boolean openType = TypeComputerUtilities.isOpenType(f);
        int n = f.getArguments().size();
        if (n == 0 || openType) {
            return new AOrderedListType(BuiltinType.ANY, null);
        } else {
            ArrayList<IAType> types = new ArrayList<IAType>();
            for (int k = 0; k < f.getArguments().size(); k++) {
                IAType type = (IAType) env.getType(f.getArguments().get(k).getValue());
                if (type.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) type))
                    type = ((AUnionType) type).getUnionList()
                            .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                if (types.indexOf(type) < 0) {
                    types.add(type);
                }
            }
            if (types.size() == 1) {
                return new AOrderedListType(types.get(0), null);
            } else {
                throw new AlgebricksException("You can not construct a heterogenous list.");
            }
        }
    }
}
