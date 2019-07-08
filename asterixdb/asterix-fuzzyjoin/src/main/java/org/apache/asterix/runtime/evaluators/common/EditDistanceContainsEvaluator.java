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
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;

public class EditDistanceContainsEvaluator extends EditDistanceCheckEvaluator {

    public EditDistanceContainsEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(args, context, sourceLoc);
    }

    @Override
    protected int computeResult(IPointable left, IPointable right, ATypeTag argType) throws HyracksDataException {
        byte[] leftBytes = left.getByteArray();
        int leftStartOffset = left.getStartOffset();
        byte[] rightBytes = right.getByteArray();
        int rightStartOffset = right.getStartOffset();

        switch (argType) {
            case STRING: {
                return ed.UTF8StringEditDistanceContains(leftBytes, leftStartOffset + typeIndicatorSize, rightBytes,
                        rightStartOffset + typeIndicatorSize, edThresh);
            }
            case ARRAY: {
                firstOrdListIter.reset(leftBytes, leftStartOffset);
                secondOrdListIter.reset(rightBytes, rightStartOffset);
                return ed.getSimilarityContains(firstOrdListIter, secondOrdListIter, edThresh);
            }
            default: {
                throw new TypeMismatchException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE_CONTAINS, 0,
                        argType.serialize(), ATypeTag.SERIALIZED_STRING_TYPE_TAG,
                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
            }
        }
    }
}
