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

import java.io.DataOutput;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.LogRedactionUtil;

public abstract class AbstractConstructorEvaluator implements IScalarEvaluator {

    protected final IEvaluatorContext ctx;
    protected final IScalarEvaluator inputEval;
    protected final SourceLocation sourceLoc;
    protected final IPointable inputArg;
    protected final ArrayBackedValueStorage resultStorage;
    protected final DataOutput out;

    protected AbstractConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
            SourceLocation sourceLoc) {
        this.ctx = ctx;
        this.inputEval = inputEval;
        this.sourceLoc = sourceLoc;
        this.inputArg = new VoidPointable();
        this.resultStorage = new ArrayBackedValueStorage();
        this.out = resultStorage.getDataOutput();
    }

    @Override
    public final void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        evaluateInput(tuple);
        if (checkAndSetMissingOrNull(result)) {
            return;
        }
        evaluateImpl(result);
    }

    protected void evaluateInput(IFrameTupleReference tuple) throws HyracksDataException {
        inputEval.evaluate(tuple, inputArg);
    }

    protected boolean checkAndSetMissingOrNull(IPointable result) throws HyracksDataException {
        return PointableHelper.checkAndSetMissingOrNull(result, inputArg);
    }

    protected abstract void evaluateImpl(IPointable result) throws HyracksDataException;

    protected abstract FunctionIdentifier getIdentifier();

    protected abstract BuiltinType getTargetType();

    protected void handleUnsupportedType(ATypeTag inputType, IPointable result) throws HyracksDataException {
        warnUnsupportedType(inputType);
        PointableHelper.setNull(result);
    }

    protected void warnUnsupportedType(ATypeTag inputType) {
        ExceptionUtil.warnUnsupportedType(ctx, sourceLoc, getIdentifier().getName() + "()", inputType);
    }

    protected void handleParseError(UTF8StringPointable textPtr, IPointable result) {
        warnParseError(textPtr);
        PointableHelper.setNull(result);
    }

    protected void warnParseError(UTF8StringPointable textPtr) {
        IWarningCollector warningCollector = ctx.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(sourceLoc, ErrorCode.INVALID_FORMAT,
                    getTargetType().getTypeTag().toString(), LogRedactionUtil.userData(textPtr.toString())));
        }
    }
}
