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

package org.apache.asterix.external.operators;

import static org.msgpack.core.MessagePack.Code.ARRAY16;
import static org.msgpack.core.MessagePack.Code.ARRAY32;
import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;
import static org.msgpack.core.MessagePack.Code.isFixedArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.library.PythonLibraryEvaluator;
import org.apache.asterix.external.library.PythonLibraryEvaluatorFactory;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

public final class ExternalAssignBatchRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private int[] outColumns;
    private final IExternalFunctionDescriptor[] fnDescs;
    private final int[][] fnArgColumns;

    private int rpcBufferSize;

    public ExternalAssignBatchRuntimeFactory(int[] outColumns, IExternalFunctionDescriptor[] fnDescs,
            int[][] fnArgColumns, int[] projectionList) {
        super(projectionList);
        this.outColumns = outColumns;
        this.fnDescs = fnDescs;
        this.fnArgColumns = fnArgColumns;
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {

        final int[] projectionToOutColumns = new int[projectionList.length];
        //this is a temporary bodge. these buffers need to work like vsize frames, or be absent entirely
        int maxArgSz = ExternalDataUtils.getArgBufferSize();
        rpcBufferSize = ExternalDataUtils.roundUpToNearestFrameSize(maxArgSz, ctx.getInitialFrameSize());
        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private ByteBuffer outputWrapper;
            private List<ByteBuffer> argHolders;
            ArrayTupleBuilder tupleBuilder;
            private List<Pair<Long, PythonLibraryEvaluator>> libraryEvaluators;
            private ATypeTag[][] nullCalls;
            private int[] numCalls;
            private VoidPointable ref;
            private MessageUnpacker unpacker;
            private ArrayBufferInput unpackerInput;
            private List<Pair<ByteBuffer, Counter>> batchResults;

            @Override
            public void open() throws HyracksDataException {
                super.open();
                initAccessAppend(ctx);
                tupleBuilder = new ArrayTupleBuilder(projectionList.length);
                tRef = new FrameTupleReference();
                ref = VoidPointable.FACTORY.createPointable();
                libraryEvaluators = new ArrayList<>();
                try {
                    PythonLibraryEvaluatorFactory evalFactory = new PythonLibraryEvaluatorFactory(ctx);
                    for (IExternalFunctionDescriptor fnDesc : fnDescs) {
                        PythonLibraryEvaluator eval = evalFactory.getEvaluator(fnDesc.getFunctionInfo(), sourceLoc);
                        long id = eval.initialize(fnDesc.getFunctionInfo());
                        libraryEvaluators.add(new Pair<>(id, eval));
                    }
                } catch (IOException | AsterixException e) {
                    throw RuntimeDataException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION, e, sourceLoc, e.getMessage());
                }
                argHolders = new ArrayList<>(fnArgColumns.length);
                for (int i = 0; i < fnArgColumns.length; i++) {
                    argHolders.add(ctx.allocateFrame(rpcBufferSize));
                }
                outputWrapper = ctx.allocateFrame();
                nullCalls = new ATypeTag[argHolders.size()][0];
                numCalls = new int[fnArgColumns.length];
                batchResults = new ArrayList<>(argHolders.size());
                for (int i = 0; i < argHolders.size(); i++) {
                    batchResults.add(new Pair<>(ctx.allocateFrame(rpcBufferSize), new Counter(-1)));
                }
                unpackerInput = new ArrayBufferInput(new byte[0]);
                unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
            }

            private void resetBuffers(int numTuples, int[] numCalls) {
                for (int func = 0; func < fnArgColumns.length; func++) {
                    argHolders.get(func).clear();
                    argHolders.get(func).position(0);
                    if (nullCalls[func].length < numTuples) {
                        nullCalls[func] = new ATypeTag[numTuples];
                    }
                    numCalls[func] = numTuples;
                    Arrays.fill(nullCalls[func], ATypeTag.TYPE);
                    for (Pair<ByteBuffer, Counter> batch : batchResults) {
                        batch.getFirst().clear();
                        batch.getFirst().position(0);
                        batch.getSecond().set(-1);
                    }
                }
            }

            private ATypeTag handleNullMatrix(int func, int t, ATypeTag argumentPresence, ATypeTag argumentStatus) {
                //If any argument is unknown, skip call. If any argument is null, return null, first.
                //However, if any argument is missing, return missing instead.
                if (nullCalls[func][t] == ATypeTag.TYPE && argumentPresence != ATypeTag.TYPE) {
                    if (argumentPresence == ATypeTag.NULL && argumentStatus != ATypeTag.MISSING) {
                        nullCalls[func][t] = argumentPresence;
                        return ATypeTag.NULL;
                    } else {
                        nullCalls[func][t] = argumentPresence;
                        return ATypeTag.MISSING;
                    }
                }
                return argumentPresence;
            }

            private void collectFunctionWarnings(List<Pair<ByteBuffer, Counter>> batchResults) throws IOException {
                for (Pair<ByteBuffer, Counter> result : batchResults) {
                    if (result.getSecond().get() > -1) {
                        ByteBuffer resBuf = result.getFirst();
                        unpackerInput.reset(resBuf.array(), resBuf.position() + resBuf.arrayOffset(),
                                resBuf.remaining());
                        unpacker.reset(unpackerInput);
                        try {
                            int numEntries = unpacker.unpackArrayHeader();
                            for (int j = 0; j < numEntries; j++) {
                                if (ctx.getWarningCollector().shouldWarn()) {
                                    ctx.getWarningCollector().warn(Warning.of(sourceLoc,
                                            ErrorCode.EXTERNAL_UDF_EXCEPTION, unpacker.unpackString()));
                                }
                            }
                        } catch (MessagePackException e) {
                            if (ctx.getWarningCollector().shouldWarn()) {
                                ctx.getWarningCollector().warn(Warning.of(sourceLoc, ErrorCode.EXTERNAL_UDF_EXCEPTION,
                                        "Error retrieving returned warnings from Python UDF"));
                            }
                        }
                    }
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                tupleBuilder.reset();
                try {
                    int numTuples = tAccess.getTupleCount();
                    resetBuffers(numTuples, numCalls);
                    //build columns of arguments for each function
                    for (int t = 0; t < numTuples; t++) {
                        for (int func = 0; func < fnArgColumns.length; func++) {
                            tRef.reset(tAccess, t);
                            int[] cols = fnArgColumns[func];
                            //TODO: switch between fixarray/array16/array32 where appropriate
                            ATypeTag argumentStatus = ATypeTag.TYPE;
                            if (!fnDescs[func].getFunctionInfo().getNullCall()) {
                                for (int colIdx = 0; colIdx < cols.length; colIdx++) {
                                    ref.set(buffer.array(), tRef.getFieldStart(cols[colIdx]),
                                            tRef.getFieldLength(cols[colIdx]));
                                    ATypeTag argumentPresence = PythonLibraryEvaluator
                                            .peekArgument(fnDescs[func].getArgumentTypes()[colIdx], ref);
                                    argumentStatus = handleNullMatrix(func, t, argumentPresence, argumentStatus);
                                }
                            }
                            if (argumentStatus == ATypeTag.TYPE) {
                                if (cols.length > 0) {
                                    argHolders.get(func).put(ARRAY16);
                                    argHolders.get(func).putShort((short) cols.length);
                                }
                                for (int colIdx = 0; colIdx < cols.length; colIdx++) {
                                    ref.set(buffer.array(), tRef.getFieldStart(cols[colIdx]),
                                            tRef.getFieldLength(cols[colIdx]));
                                    PythonLibraryEvaluator.setArgument(fnDescs[func].getArgumentTypes()[colIdx], ref,
                                            argHolders.get(func), fnDescs[func].getFunctionInfo().getNullCall());
                                }
                            } else {
                                numCalls[func]--;
                            }
                            if (cols.length == 0) {
                                PythonLibraryEvaluator.setVoidArgument(argHolders.get(func));
                            }
                        }
                    }
                    //TODO: maybe this could be done in parallel for each unique library evaluator?
                    for (int argHolderIdx = 0; argHolderIdx < argHolders.size(); argHolderIdx++) {
                        Pair<Long, PythonLibraryEvaluator> fnEval = libraryEvaluators.get(argHolderIdx);
                        ByteBuffer columnResult = fnEval.getSecond().callPythonMulti(fnEval.getFirst(),
                                argHolders.get(argHolderIdx), numCalls[argHolderIdx]);
                        if (columnResult != null) {
                            Pair<ByteBuffer, Counter> resultholder = batchResults.get(argHolderIdx);
                            if (resultholder.getFirst().capacity() < columnResult.capacity()) {
                                resultholder.setFirst(ctx.allocateFrame(ExternalDataUtils.roundUpToNearestFrameSize(
                                        columnResult.capacity(), ctx.getInitialFrameSize())));
                            }
                            ByteBuffer resultBuf = resultholder.getFirst();
                            resultBuf.clear();
                            resultBuf.position(0);
                            //offset 1 to skip message type
                            System.arraycopy(columnResult.array(), columnResult.arrayOffset() + 1, resultBuf.array(),
                                    resultBuf.arrayOffset(), columnResult.capacity() - 1);
                            //wrapper for results and warnings arrays. always length 2
                            consumeAndGetBatchLength(resultBuf);
                            int numResults = (int) consumeAndGetBatchLength(resultBuf);
                            resultholder.getSecond().set(numResults);
                        } else {
                            if (ctx.getWarningCollector().shouldWarn()) {
                                ctx.getWarningCollector()
                                        .warn(Warning.of(sourceLoc, ErrorCode.EXTERNAL_UDF_EXCEPTION,
                                                "Function "
                                                        + fnDescs[argHolderIdx].getFunctionInfo()
                                                                .getFunctionIdentifier().toString()
                                                        + " failed to execute"));
                            }
                        }
                    }
                    //decompose returned function columns into frame tuple format
                    for (int i = 0; i < numTuples; i++) {
                        tupleBuilder.reset();
                        for (int f = 0; f < projectionList.length; f++) {
                            int k = projectionToOutColumns[f];
                            if (k >= 0) {
                                outputWrapper.clear();
                                outputWrapper.position(0);
                                Pair<ByteBuffer, Counter> result = batchResults.get(k);
                                if (result.getFirst() != null) {
                                    if (result.getFirst().capacity() > outputWrapper.capacity()) {
                                        outputWrapper = ctx.allocateFrame(ExternalDataUtils.roundUpToNearestFrameSize(
                                                outputWrapper.capacity(), ctx.getInitialFrameSize()));
                                    }
                                }
                                int start = outputWrapper.arrayOffset();
                                ATypeTag functionCalled = nullCalls[k][i];
                                if (functionCalled == ATypeTag.TYPE) {
                                    if (result.getSecond().get() > 0) {
                                        MessageUnpackerToADM.unpack(result.getFirst(), outputWrapper, true);
                                        result.getSecond().set(result.getSecond().get() - 1);
                                    } else {
                                        //emit NULL for functions which failed with a warning
                                        outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                                    }
                                } else if (functionCalled == ATypeTag.NULL) {
                                    outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                                } else {
                                    outputWrapper.put(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                                }
                                tupleBuilder.addField(outputWrapper.array(), start, start + outputWrapper.position());
                            } else {
                                tupleBuilder.addField(tAccess, i, projectionList[f]);
                            }
                        }
                        appendToFrameFromTupleBuilder(tupleBuilder);
                    }
                    collectFunctionWarnings(batchResults);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            private long consumeAndGetBatchLength(ByteBuffer buf) {
                byte tag = buf.get();
                if (isFixedArray(tag)) {
                    return tag ^ FIXARRAY_PREFIX;
                } else if (tag == ARRAY16) {
                    return Short.toUnsignedInt(buf.getShort());
                } else if (tag == ARRAY32) {
                    return Integer.toUnsignedLong(buf.getInt());
                }
                return -1L;
            }

            @Override
            public void flush() throws HyracksDataException {
                appender.flush(writer);
            }
        };
    }
}
