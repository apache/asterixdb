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
 * record merge evaluator is used to combine two records with no matching fieldnames
 * If both records have the same fieldname for a non-record field anywhere in the schema, the merge will fail
 * This function is performed on a recursive level, meaning that nested records can be combined
 * for instance if both records have a nested field called "metadata"
 * where metadata from A is {"comments":"this rocks"} and metadata from B is {"index":7, "priority":5}
 * Records A and B can be combined yielding a nested record called "metadata"
 * That will have all three fields
 */

@MissingNullInOutFunction
public class RecordMergeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RecordMergeDescriptor();
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
                return new RecordMergeEvaluator(ctx, args, argTypes, sourceLoc, getIdentifier(), false);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RECORD_MERGE;
    }
}
