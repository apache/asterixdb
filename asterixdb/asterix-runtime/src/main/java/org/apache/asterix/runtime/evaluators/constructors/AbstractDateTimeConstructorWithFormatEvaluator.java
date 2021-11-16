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

package org.apache.asterix.runtime.evaluators.constructors;

import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.temporal.AsterixTemporalTypeParseException;
import org.apache.asterix.om.base.temporal.DateTimeFormatUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractDateTimeConstructorWithFormatEvaluator extends AbstractDateTimeConstructorEvaluator {

    private final IScalarEvaluator formatEval;
    protected final IPointable formatArg = new VoidPointable();
    private final UTF8StringPointable formatTextPtr = new UTF8StringPointable();
    private final AMutableInt64 aInt64 = new AMutableInt64(0);

    protected AbstractDateTimeConstructorWithFormatEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(ctx, args[0].createScalarEvaluator(ctx), sourceLoc);
        formatEval = args[1].createScalarEvaluator(ctx);
    }

    @Override
    protected void evaluateInput(IFrameTupleReference tuple) throws HyracksDataException {
        super.evaluateInput(tuple);
        formatEval.evaluate(tuple, formatArg);
    }

    @Override
    protected boolean checkAndSetMissingOrNull(IPointable result) throws HyracksDataException {
        return PointableHelper.checkAndSetMissingOrNull(result, inputArg, formatArg);
    }

    @Override
    protected boolean parseDateTime(UTF8StringPointable textPtr, AMutableDateTime result) {
        byte[] formatBytes = formatArg.getByteArray();
        int formatStartOffset = formatArg.getStartOffset();
        int formatLength = formatArg.getLength();
        if (formatBytes[formatStartOffset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            return false;
        }
        formatTextPtr.set(formatBytes, formatStartOffset + 1, formatLength - 1);
        try {
            if (DateTimeFormatUtils.getInstance().parseDateTime(aInt64, textPtr.getByteArray(),
                    textPtr.getCharStartOffset(), textPtr.getUTF8Length(), formatBytes,
                    formatTextPtr.getCharStartOffset(), formatTextPtr.getUTF8Length(),
                    DateTimeFormatUtils.DateTimeParseMode.DATETIME, false)) {
                result.setValue(aInt64.getLongValue());
                return true;
            } else {
                return false;
            }
        } catch (AsterixTemporalTypeParseException e) {
            // shouldn't happen
            return false;
        }
    }
}
