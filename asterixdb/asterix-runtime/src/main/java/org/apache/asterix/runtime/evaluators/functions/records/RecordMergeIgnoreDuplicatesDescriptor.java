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
package org.apache.asterix.runtime.evaluators.functions.records;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The record merge ignore duplicates differs from the normal record merging in the following scenarios:
 * - When 2 fields have matching names, but different values, left record field will be taken.
 * - When 2 fields have matching names, but different types, left record field will be taken.
 *
 * Examples:
 * - Matching field name, type and value
 * - normal merge: {id: 1}, {id: 1} -> {id: 1}
 * - ignore merge: {id: 1}, {id: 1} -> {id: 1}
 *
 * - Matching field name, type, different value
 * - normal merge: {id: 1}, {id: 2} -> duplicate field exception (mismatched value)
 * - ignore merge: {id: 1}, {id: 2} -> {id: 1} (mismatched values are ignored, left record field is taken)
 *
 * - Matching field name, different type
 * - normal merge: {id: 1}, {id: "1"} -> duplicate field exception (mismatched type)
 * - ignore merge: {id: 1}, {id: "1"} -> {id: 1} (mismatched types are ignored, left record field is taken)
 */

@MissingNullInOutFunction
public class RecordMergeIgnoreDuplicatesDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordMergeIgnoreDuplicatesDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new FunctionTypeInferers.RecordMergeTypeInferer();
        }
    };

    private static final long serialVersionUID = 1L;
    private final IAType[] argTypes = new IAType[3];

    @Override
    public void setImmutableStates(Object... states) {
        argTypes[0] = TypeComputeUtils.extractRecordType((IAType) states[0]);
        argTypes[1] = TypeComputeUtils.extractRecordType((IAType) states[1]);
        argTypes[2] = TypeComputeUtils.extractRecordType((IAType) states[2]);
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new RecordMergeEvaluator(ctx, args, argTypes, sourceLoc, getIdentifier(), true);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RECORD_MERGE_IGNORE_DUPLICATES;
    }
}
