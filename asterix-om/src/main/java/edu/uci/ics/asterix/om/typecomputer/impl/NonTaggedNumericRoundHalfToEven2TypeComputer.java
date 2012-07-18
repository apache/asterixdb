/*
 * Numeric round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import java.util.ArrayList;
import java.util.List;

public class NonTaggedNumericRoundHalfToEven2TypeComputer implements IResultTypeComputer {

    public static final NonTaggedNumericRoundHalfToEven2TypeComputer INSTANCE =
            new NonTaggedNumericRoundHalfToEven2TypeComputer();

    private NonTaggedNumericRoundHalfToEven2TypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if(fce.getArguments().size() < 2)
        	throw new AlgebricksException("Argument number invalid.");
        
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(1).getValue();
        
        IAType t1 = (IAType) env.getType(arg1);
        IAType t2 = (IAType) env.getType(arg2);
        
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);        

        ATypeTag tag1, tag2;
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1))
            tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag1 = t1.getTypeTag();

        if (t2.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t2))
            tag2 = ((AUnionType) t2).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag2 = t2.getTypeTag();
        
        switch(tag2) {
	        case INT8:
	        case INT16:
	        case INT32:
	        case INT64:
	            break;
	        default:
	            throw new AlgebricksException("Argument $precision cannot be type " + t2.getTypeName());
        }        
        
        switch (tag1) {
            case INT8:
                unionList.add(BuiltinType.AINT8);
                break;                
            case INT16:
                unionList.add(BuiltinType.AINT16);
                break;                 
            case INT32:
                unionList.add(BuiltinType.AINT32);
                break;                 
            case INT64:
                unionList.add(BuiltinType.AINT64);
                break;                 
            case FLOAT:
                unionList.add(BuiltinType.AFLOAT);
                break;                 
            case DOUBLE:
                unionList.add(BuiltinType.ADOUBLE);
                break;
            case NULL:
                return BuiltinType.ANULL;
            default: {
                throw new NotImplementedException("Arithmetic operations are not implemented for " + t1.getTypeName());
            }
        }

        return new AUnionType(unionList, "NumericFuncionsResult");
    }
}
