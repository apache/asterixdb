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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class RecordTransformEvaluator extends RecordMergeEvaluator {

    RecordTransformEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args, IAType[] argTypes,
            SourceLocation sourceLocation, FunctionIdentifier identifier, boolean isRecursiveRemoval)
            throws HyracksDataException {
        super(ctx, args, argTypes, sourceLocation, identifier, true);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, argPtr0);
        eval1.evaluate(tuple, argPtr1);
        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
            return;
        }
        ATypeTag argRightTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset()]);
        ATypeTag argLeftTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr0.getByteArray()[argPtr0.getStartOffset()]);
        if (argRightTag != ATypeTag.OBJECT || argLeftTag != ATypeTag.OBJECT) {
            result.set(argPtr0);
            return;
        }
        super.evaluateImpl(result);
    }
}
