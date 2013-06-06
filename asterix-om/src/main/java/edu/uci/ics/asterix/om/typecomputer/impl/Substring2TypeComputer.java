/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
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


public class Substring2TypeComputer implements IResultTypeComputer {
    public static final Substring2TypeComputer INSTANCE = new Substring2TypeComputer();
    
    
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if(fce.getArguments().size() < 2)
            throw new AlgebricksException("Wrong Argument Number.");            
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg1 = fce.getArguments().get(1).getValue();       
        IAType t0, t1;
        try {
            t0 = (IAType) env.getType(arg0);
            t1 = (IAType) env.getType(arg1);                  
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        
        ATypeTag tag0, tag1;
        if (t0.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t0))
            tag0 = ((AUnionType) t0).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag0 = t0.getTypeTag();
        
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1))
            tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag1 = t1.getTypeTag();        
        
        if(tag0 != ATypeTag.NULL && tag0 != ATypeTag.STRING) {
            throw new AlgebricksException("First argument should be String Type.");
        }
        
        if(tag1 != ATypeTag.NULL && 
           tag1 != ATypeTag.INT8 && 
           tag1 != ATypeTag.INT16 && 
           tag1 != ATypeTag.INT32 && 
           tag1 != ATypeTag.INT64) {
            throw new AlgebricksException("Second argument should be integer Type.");
        }

        return BuiltinType.ASTRING;
    }   
}
