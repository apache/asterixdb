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

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Type computer for record-transform function used in UPDATE SET operations.
 * Computes the output record type after updating/adding fields to an input record.
 * Function: record-transform(transformationRecord, originalRecord)
 * - transformationRecord (t0): Record with new/updated field values. Can be MISSING.
 * - originalRecord (t1): Original record type to be updated.
 * Delegates to RecordMergeTypeComputer with MERGE_ON_TRANSFORM_RECORDS flag to merge
 * transformation fields into the original record. Updates existing fields and adds new ones.
 * Edge cases:
 * - If either argument is not a record: return t1 if t0 can be MISSING, otherwise return t0.
 */
public class RecordTransformTypeComputer implements IResultTypeComputer {

    public static final RecordTransformTypeComputer INSTANCE = new RecordTransformTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());
        ARecordType recType0 = TypeComputeUtils.extractRecordType(t0);
        ARecordType recType1 = TypeComputeUtils.extractRecordType(t1);
        IAType resultType = t0;
        if ((recType0 == null || recType1 == null)) {
            if (TypeHelper.canBeMissing(t0)) {
                resultType = t1;
            }
            return resultType;
        }

        /** Infer merged type only if t0 is a record of type 'transform' and t1 is a record */
        resultType = RecordMergeTypeComputer.INSTANCE_IGNORE_DUPLICATES_MERGE_ON_TRANSFORM_RECORDS
                .computeType(expression, env, metadataProvider);
        return resultType;
    }
}
