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
 * Numeric Unary Functions like abs
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
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

public class NonTaggedNumericUnaryFunctionTypeComputer implements IResultTypeComputer {

    private static final String errMsg = "Arithmetic operations are not implemented for ";
    public static final NonTaggedNumericUnaryFunctionTypeComputer INSTANCE = new NonTaggedNumericUnaryFunctionTypeComputer();

    private NonTaggedNumericUnaryFunctionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().isEmpty())
            throw new AlgebricksException("Wrong Argument Number.");

        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();

        IAType t = (IAType) env.getType(arg1);

        if (NonTaggedFormatUtil.isOptional(t)) {
            return (IAType) env.getType(arg1);
        }

        IAType type;
        switch (t.getTypeTag()) {
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
            case ANY:
                return BuiltinType.ANY;
            default: {
                throw new NotImplementedException(errMsg + t.getTypeName());
            }
        }

        return AUnionType.createNullableType(type, "NumericUnaryFuncionsResult");
    }
}
