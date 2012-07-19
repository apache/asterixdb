package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;


/**
 *
 * @author Xiaoyu Ma
 */
public class UnaryStringOrNullTypeComputer implements IResultTypeComputer  {   
    
    public static final UnaryStringOrNullTypeComputer INSTANCE = new UnaryStringOrNullTypeComputer();
    private UnaryStringOrNullTypeComputer() {}
    
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if(fce.getArguments().isEmpty())
            throw new AlgebricksException("Wrong Argument Number.");        
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        IAType t0;
        try {
            t0 = (IAType) env.getType(arg0);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        
        if (TypeHelper.canBeNull(t0)) {
            return AUnionType.createNullableType(BuiltinType.ASTRING);
        }            
        
        if (t0.getTypeTag() == ATypeTag.NULL)
        	return BuiltinType.ANULL;
        
        if(t0.getTypeTag() == ATypeTag.STRING) 
        	return BuiltinType.ASTRING;
        
        throw new AlgebricksException("Expects String Type.");        
    }      
}
