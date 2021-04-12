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

package org.apache.asterix.external.library;

import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final ByteBuffer argHolder;
    private final ByteBuffer outputWrapper;
    private final IEvaluatorContext evaluatorContext;

    private final IPointable[] argValues;
    private final SourceLocation sourceLocation;

    private MessageUnpacker unpacker;
    private ArrayBufferInput unpackerInput;

    private long fnId;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        try {
            PythonLibraryEvaluatorFactory evaluatorFactory = new PythonLibraryEvaluatorFactory(ctx.getTaskContext());
            this.libraryEvaluator = evaluatorFactory.getEvaluator(finfo, sourceLoc);
            this.fnId = libraryEvaluator.initialize(finfo);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        //TODO: these should be dynamic
        this.argHolder = ByteBuffer.wrap(new byte[Short.MAX_VALUE * 2]);
        this.outputWrapper = ByteBuffer.wrap(new byte[Short.MAX_VALUE * 2]);
        this.evaluatorContext = ctx;
        this.sourceLocation = sourceLoc;
        this.unpackerInput = new ArrayBufferInput(new byte[0]);
        this.unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argHolder.clear();
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            if (!finfo.getNullCall() && PointableHelper.checkAndSetMissingOrNull(result, argValues[i])) {
                return;
            }
            try {
                PythonLibraryEvaluator.setArgument(argTypes[i], argValues[i], argHolder, finfo.getNullCall());
            } catch (IOException e) {
                throw new HyracksDataException("Error evaluating Python UDF", e);
            }
        }
        try {
            ByteBuffer res = libraryEvaluator.callPython(fnId, argHolder, argTypes.length);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer);
    }

    private void wrap(ByteBuffer resultWrapper, DataOutput out) throws HyracksDataException {
        //TODO: output wrapper needs to grow with result wrapper
        outputWrapper.clear();
        outputWrapper.position(0);
        try {
            if (resultWrapper == null) {
                outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
                return;
            }
            if ((resultWrapper.get() ^ FIXARRAY_PREFIX) != (byte) 2) {
                throw HyracksDataException.create(AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                        "Returned result missing outer wrapper"));
            }
            int numresults = resultWrapper.get() ^ FIXARRAY_PREFIX;
            if (numresults > 0) {
                MessageUnpackerToADM.unpack(resultWrapper, outputWrapper, true);
            }
            unpackerInput.reset(resultWrapper.array(), resultWrapper.position() + resultWrapper.arrayOffset(),
                    resultWrapper.remaining());
            unpacker.reset(unpackerInput);
            int numEntries = unpacker.unpackArrayHeader();
            for (int j = 0; j < numEntries; j++) {
                outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                if (evaluatorContext.getWarningCollector().shouldWarn()) {
                    evaluatorContext.getWarningCollector().warn(
                            Warning.of(sourceLocation, ErrorCode.EXTERNAL_UDF_EXCEPTION, unpacker.unpackString()));
                }
            }
            out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
