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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Type computer for record-remove-recursive function used in UPDATE REMOVE operations.
 * Computes the output record type after removing specified fields from an input record.
 * Function: record-remove-recursive(removalSpec, originalRecord)
 * - removalSpec (t0): Record with MISSING fields indicating what to remove. Must be OBJECT.
 * - originalRecord (t1): Original record type. Can be OBJECT, UNION (nullable), or ANY.
 * Delegates to RecordMergeTypeComputer with HANDLE_DELETIONS flag to remove fields marked as MISSING.
 * Type handling:
 * - OBJECT: Compute removal directly
 * - UNION: Compute removal on inner record type, preserve union structure
 *   (e.g., (record|null) becomes (updatedRecord|null) where updatedRecord has fields removed)
 * - ANY: Return ANY unchanged
 */
public class RecordRemoveRecursiveTypeComputer implements IResultTypeComputer {

    public static final RecordRemoveRecursiveTypeComputer INSTANCE = new RecordRemoveRecursiveTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier funcId = f.getFunctionIdentifier();
        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());

        if (t0.getTypeTag() != ATypeTag.OBJECT) {
            throw new TypeMismatchException(f.getSourceLocation(), funcId, 0, t0.getTypeTag(), ATypeTag.OBJECT);
        }

        switch (t1.getTypeTag()) {
            case OBJECT:
                return RecordMergeTypeComputer.INSTANCE_IGNORE_DUPLICATES_HANDLE_DELETIONS.computeType(expression, env,
                        metadataProvider);
            case UNION:
                IAType innerType = ((AUnionType) t1).getActualType();
                if (innerType.getTypeTag() == ATypeTag.OBJECT) {
                    IAType mergedType = RecordMergeTypeComputer.INSTANCE_IGNORE_DUPLICATES_HANDLE_DELETIONS
                            .computeType(expression, env, metadataProvider);

                    List<IAType> newUnionList = new ArrayList<>();
                    AUnionType currUnionType = (AUnionType) t1;
                    newUnionList.addAll(currUnionType.getUnionList());
                    AUnionType newUnionType = new AUnionType(newUnionList, "union-post-remove-recursive");
                    newUnionType.setActualType(mergedType);
                    return newUnionType;
                } else {
                    return t1;
                }
            case ANY:
                return t1;
        }
        return t1;
    }
}
