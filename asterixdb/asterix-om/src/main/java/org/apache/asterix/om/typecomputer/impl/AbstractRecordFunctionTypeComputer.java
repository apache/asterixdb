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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Base type computer for the following record-functions:
 * 1. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_ADD}
 * 2. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_PUT}
 * 3. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_REMOVE}
 * 4. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_RENAME}
 */
public abstract class AbstractRecordFunctionTypeComputer implements IResultTypeComputer {
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) expression;

        // Get our record type.
        ILogicalExpression arg0 = functionCallExpression.getArguments().get(0).getValue();
        IAType type0 = (IAType) env.getType(arg0);
        IAType actualType0 = TypeComputeUtils.getActualType(type0);
        ATypeTag tag0 = actualType0.getTypeTag();
        if (tag0 == ATypeTag.ANY) {
            return wrapTypeWithUnknown(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, true, true);
        } else if (tag0 == ATypeTag.MISSING) {
            // Our output is always going to be MISSING.
            return BuiltinType.AMISSING;
        } else if (tag0 != ATypeTag.OBJECT) {
            // Our output is always going to be NULL.
            return BuiltinType.ANULL;
        }
        boolean isOutputMissable = TypeHelper.canBeMissing(type0);
        boolean isOutputNullable = TypeHelper.canBeNull(type0);
        ARecordType inputRecordType = TypeComputeUtils.extractRecordType(actualType0);

        // Our second argument should be of type "string".
        ILogicalExpression arg1 = functionCallExpression.getArguments().get(1).getValue();
        IAType type1 = (IAType) env.getType(arg1);
        IAType actualType1 = TypeComputeUtils.getActualType(type1);
        ATypeTag tag1 = actualType1.getTypeTag();
        if (tag1 == ATypeTag.ANY) {
            // We cannot infer the type of our second argument-- our output may be MISSING or NULL.
            return wrapTypeWithUnknown(type0, true, true);
        } else if (tag1 == ATypeTag.MISSING) {
            // Our output is always going to be MISSING.
            return BuiltinType.AMISSING;
        } else if (tag1 != ATypeTag.STRING) {
            // Our output is always going to be NULL.
            return BuiltinType.ANULL;
        }
        isOutputMissable |= TypeHelper.canBeMissing(type1);
        isOutputNullable |= TypeHelper.canBeNull(type1);

        // Compute our type.
        return computeTypeImpl(functionCallExpression, env, inputRecordType, isOutputMissable, isOutputNullable);
    }

    protected abstract IAType computeTypeImpl(AbstractFunctionCallExpression functionCallExpression,
            IVariableTypeEnvironment env, ARecordType inputRecordType, boolean isOutputMissable,
            boolean isOutputNullable) throws AlgebricksException;

    protected static IAType wrapTypeWithUnknown(IAType originalType, boolean isMissable, boolean isNullable) {
        if (isNullable && isMissable) {
            return AUnionType.createUnknownableType(originalType);
        } else if (isNullable) { // && !isMissable
            return AUnionType.createNullableType(originalType);
        } else if (isMissable) { // && !isNullable
            return AUnionType.createMissableType(originalType);
        } else {
            return originalType;
        }
    }
}
