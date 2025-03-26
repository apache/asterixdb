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

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.Result;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;

public abstract class AbstractValueComparisonEvaluator extends AbstractComparisonEvaluator {
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public AbstractValueComparisonEvaluator(IScalarEvaluatorFactory evalLeftFactory, IAType leftType,
            IScalarEvaluatorFactory evalRightFactory, IAType rightType, IEvaluatorContext ctx, SourceLocation sourceLoc,
            boolean isEquality) throws HyracksDataException {
        super(evalLeftFactory, leftType, evalRightFactory, rightType, ctx, sourceLoc, isEquality);
    }

    @Override
    protected void evaluateImpl(IPointable result) throws HyracksDataException {
        Result comparisonResult = compare();
        switch (comparisonResult) {
            case MISSING:
                writeMissing(result);
                break;
            case NULL:
                writeNull(result);
                break;
            case INCOMPARABLE:
                ExceptionUtil.warnIncomparableTypes(ctx, sourceLoc, VALUE_TYPE_MAPPING[argLeft.getTag()],
                        VALUE_TYPE_MAPPING[argRight.getTag()]);
                handleIncomparable(result);
                break;
            default:
                resultStorage.reset();
                ABoolean b = ABoolean.valueOf(getComparisonResult(comparisonResult));
                serde.serialize(b, out);
                result.set(resultStorage);
        }
    }

    protected abstract boolean getComparisonResult(Result r);

    protected abstract void handleIncomparable(IPointable result) throws HyracksDataException;

}
