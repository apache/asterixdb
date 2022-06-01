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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.ILibraryEvaluator;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.external.util.ExternalDataUtils;
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

    private final ILibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final ByteBuffer argHolder;
    private final ByteBuffer outputWrapper;
    private final IEvaluatorContext evaluatorContext;

    private final IPointable[] argValues;
    private final SourceLocation sourceLocation;

    private MessageUnpacker unpacker;
    private ArrayBufferInput unpackerInput;
    private MessageUnpackerToADM unpackerToADM;

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
        //TODO: these should be dynamic. this static size picking is a temporary bodge until this works like
        //      v-size frames do or these construction buffers are removed entirely
        int maxArgSz = ExternalDataUtils.getArgBufferSize();
        this.argHolder = ByteBuffer.wrap(new byte[maxArgSz]);
        this.outputWrapper = ByteBuffer.wrap(new byte[maxArgSz]);
        this.evaluatorContext = ctx;
        this.sourceLocation = sourceLoc;
        this.unpackerInput = new ArrayBufferInput(new byte[0]);
        this.unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
        this.unpackerToADM = new MessageUnpackerToADM();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argHolder.clear();
        boolean nullCall = finfo.getNullCall();
        boolean hasNullArg = false;
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            if (!nullCall) {
                byte[] argBytes = argValues[i].getByteArray();
                int argStart = argValues[i].getStartOffset();
                ATypeTag argType = ATYPETAGDESERIALIZER.deserialize(argBytes[argStart]);
                if (argType == ATypeTag.MISSING) {
                    PointableHelper.setMissing(result);
                    return;
                } else if (argType == ATypeTag.NULL) {
                    hasNullArg = true;
                }
            }
        }
        if (!nullCall && hasNullArg) {
            PointableHelper.setNull(result);
            return;
        }
        try {
            ByteBuffer res = libraryEvaluator.call(fnId, argTypes, argValues, nullCall);
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
                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                return;
            }
            if ((resultWrapper.get() ^ FIXARRAY_PREFIX) != (byte) 2) {
                throw HyracksDataException
                        .create(AsterixException.create(ErrorCode.EXTERNAL_UDF_PROTO_RETURN_EXCEPTION));
            }
            int numresults = resultWrapper.get() ^ FIXARRAY_PREFIX;
            if (numresults > 0) {
                unpackerToADM.unpack(resultWrapper, out, true);
            }
            unpackerInput.reset(resultWrapper.array(), resultWrapper.position() + resultWrapper.arrayOffset(),
                    resultWrapper.remaining());
            unpacker.reset(unpackerInput);
            int numErrors = unpacker.unpackArrayHeader();
            for (int j = 0; j < numErrors; j++) {
                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                if (evaluatorContext.getWarningCollector().shouldWarn()) {
                    evaluatorContext.getWarningCollector().warn(
                            Warning.of(sourceLocation, ErrorCode.EXTERNAL_UDF_EXCEPTION, unpacker.unpackString()));
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
