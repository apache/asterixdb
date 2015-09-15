/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Numeric round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedNumericRoundHalfToEven2TypeComputer implements IResultTypeComputer {

    public static final NonTaggedNumericRoundHalfToEven2TypeComputer INSTANCE = new NonTaggedNumericRoundHalfToEven2TypeComputer();

    private NonTaggedNumericRoundHalfToEven2TypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 2)
            throw new AlgebricksException("Argument number invalid.");

        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(1).getValue();

        IAType t1 = (IAType) env.getType(arg1);
        IAType t2 = (IAType) env.getType(arg2);

        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);

        ATypeTag tag1, tag2;
        if (NonTaggedFormatUtil.isOptional(t1))
            tag1 = ((AUnionType) t1).getNullableType().getTypeTag();
        else
            tag1 = t1.getTypeTag();

        if (NonTaggedFormatUtil.isOptional(t2))
            tag2 = ((AUnionType) t2).getNullableType().getTypeTag();
        else
            tag2 = t2.getTypeTag();

        switch (tag2) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                break;
            default:
                throw new AlgebricksException("Argument $precision cannot be type " + t2.getTypeName());
        }

        IAType type;
        switch (tag1) {
            case INT8:
                type = BuiltinType.AINT8;
                break;
            case INT16:
                type = BuiltinType.AINT16;
                break;
            case INT32:
                type = BuiltinType.AINT32;
                break;
            case INT64:
                type = BuiltinType.AINT64;
                break;
            case FLOAT:
                type = BuiltinType.AFLOAT;
                break;
            case DOUBLE:
                type = BuiltinType.ADOUBLE;
                break;
            case NULL:
                return BuiltinType.ANULL;
            default: {
                throw new NotImplementedException("Arithmetic operations are not implemented for " + t1.getTypeName());
            }
        }

        return AUnionType.createNullableType(type, "NumericFuncionsResult");
    }
}
