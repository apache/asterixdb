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
/*
 * Numeric Unary Functions like abs
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
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

public class NonTaggedNumericUnaryFunctionTypeComputer implements IResultTypeComputer {

    private static final String errMsg = "Arithmetic operations are not implemented for ";
    public static final NonTaggedNumericUnaryFunctionTypeComputer INSTANCE =
            new NonTaggedNumericUnaryFunctionTypeComputer();

    private NonTaggedNumericUnaryFunctionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if(fce.getArguments().isEmpty())
            throw new AlgebricksException("Wrong Argument Number.");
        
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();

        IAType t = (IAType) env.getType(arg1);
        ATypeTag tag = t.getTypeTag();

        if (tag == ATypeTag.UNION
                && NonTaggedFormatUtil.isOptionalField((AUnionType) env.getType(arg1))) {
            return (IAType) env.getType(arg1);
        }
        
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        switch (tag) {
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
                throw new NotImplementedException(errMsg + t.getTypeName());
            }
        }

        return new AUnionType(unionList, "NumericUnaryFuncionsResult");
    }
}
