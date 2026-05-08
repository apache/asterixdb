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
package org.apache.asterix.runtime.evaluators.comparisons;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.Result;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;

public class EqualsDescriptorFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory leftEvalFactory;
    private final IScalarEvaluatorFactory rightEvalFactory;
    private final IAType leftType;
    private final IAType rightType;
    private final SourceLocation sourceLoc;
    private final boolean leftIsConstant;
    private final boolean rightIsConstant;

    public EqualsDescriptorFactory(IScalarEvaluatorFactory leftEvalFactory, IAType leftType,
            IScalarEvaluatorFactory rightEvalFactory, IAType rightType, SourceLocation sourceLoc) {
        this.leftEvalFactory = leftEvalFactory;
        this.leftType = leftType;
        this.rightEvalFactory = rightEvalFactory;
        this.rightType = rightType;
        this.sourceLoc = sourceLoc;
        this.leftIsConstant = leftEvalFactory instanceof ConstantEvalFactory;
        this.rightIsConstant = rightEvalFactory instanceof ConstantEvalFactory;
    }

    public IScalarEvaluatorFactory getConstantFactory() {
        return leftIsConstant ? leftEvalFactory : rightIsConstant ? rightEvalFactory : null;
    }

    public IScalarEvaluatorFactory getExpressionFactory() {
        return leftIsConstant ? rightEvalFactory : leftEvalFactory;
    }

    public IAType getConstantType() {
        return leftIsConstant ? leftType : rightIsConstant ? rightType : null;
    }

    public IAType getExpressionType() {
        return leftIsConstant ? rightType : leftType;
    }

    public boolean isLeftConstant() {
        return leftIsConstant;
    }

    public boolean isRightConstant() {
        return rightIsConstant;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        return new AbstractValueComparisonEvaluator(leftEvalFactory, leftType, rightEvalFactory, rightType, ctx,
                sourceLoc, true) {

            @Override
            protected boolean getComparisonResult(Result r) {
                return r == Result.EQ;
            }

            @Override
            protected void handleIncomparable(IPointable result) throws HyracksDataException {
                resultStorage.reset();
                serde.serialize(ABoolean.FALSE, out);
                result.set(resultStorage);
            }
        };
    }
}
