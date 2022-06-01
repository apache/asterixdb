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
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.asterix.external.api.ILibraryEvaluator;
import org.apache.asterix.external.library.PythonLibraryEvaluatorFactory;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.external.library.msgpack.MsgPackPointableVisitor;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

public final class ExternalAssignBatchRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final int[] outColumns;
    private final IExternalFunctionDescriptor[] fnDescs;
    private final int[][] fnArgColumns;

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
        for (int j = 0; j < projectionList.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(outColumns, projectionList[j]);
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private ArrayBackedValueStorage outputWrapper;
            private List<ArrayBackedValueStorage> argHolders;
            ArrayTupleBuilder tupleBuilder;
            private List<Pair<Long, ILibraryEvaluator>> libraryEvaluators;
            private ATypeTag[][] nullCalls;
            private int[] numCalls;
            private VoidPointable ref;
            private MessageUnpacker unpacker;
            private ArrayBufferInput unpackerInput;
            private List<Pair<ByteBuffer, Counter>> batchResults;
            private MessageUnpackerToADM unpackerToADM;
            private PointableAllocator pointableAllocator;
            private MsgPackPointableVisitor pointableVisitor;
            private TaggedValuePointable anyPointer;

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
                        ILibraryEvaluator eval = evalFactory.getEvaluator(fnDesc.getFunctionInfo(), sourceLoc);
                        long id = eval.initialize(fnDesc.getFunctionInfo());
                        libraryEvaluators.add(new Pair<>(id, eval));
                    }
                } catch (IOException | AsterixException e) {
                    throw RuntimeDataException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION, e, sourceLoc, e.getMessage());
                }
                argHolders = new ArrayList<>(fnArgColumns.length);
                for (int i = 0; i < fnArgColumns.length; i++) {
                    argHolders.add(new ArrayBackedValueStorage());
                }
                outputWrapper = new ArrayBackedValueStorage();
                nullCalls = new ATypeTag[argHolders.size()][0];
                numCalls = new int[fnArgColumns.length];
                batchResults = new ArrayList<>(argHolders.size());
                for (int i = 0; i < argHolders.size(); i++) {
                    batchResults.add(new Pair<>(ByteBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE),
                            new Counter(-1)));
                }
                unpackerInput = new ArrayBufferInput(new byte[0]);
                unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
                unpackerToADM = new MessageUnpackerToADM();
                pointableAllocator = new PointableAllocator();
                pointableVisitor = new MsgPackPointableVisitor();
                anyPointer = TaggedValuePointable.FACTORY.createPointable();
            }

            private void resetBuffers(int numTuples, int[] numCalls) {
                for (int func = 0; func < fnArgColumns.length; func++) {
                    argHolders.get(func).reset();
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
                                    //TODO: in domain socket mode, a NUL can appear at the end of the stacktrace strings.
                                    //      this should probably not happen but warnings with control characters should
                                    //      also be properly escaped
                                    ctx.getWarningCollector()
                                            .warn(Warning.of(sourceLoc, ErrorCode.EXTERNAL_UDF_EXCEPTION,
                                                    unpacker.unpackString().replace('\0', ' ')));
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
                /*TODO: this whole transposition stuff is not necessary
                        the evaulator should accept a format that is a collection of rows, logically*/
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
                                    ATypeTag argumentPresence = ExternalDataUtils
                                            .peekArgument(fnDescs[func].getArgumentTypes()[colIdx], ref, anyPointer);
                                    argumentStatus = handleNullMatrix(func, t, argumentPresence, argumentStatus);
                                }
                            }
                            if (argumentStatus == ATypeTag.TYPE) {
                                if (cols.length > 0) {
                                    argHolders.get(func).getDataOutput().writeByte(ARRAY16);
                                    argHolders.get(func).getDataOutput().writeShort((short) cols.length);
                                }
                                for (int colIdx = 0; colIdx < cols.length; colIdx++) {
                                    ref.set(buffer.array(), tRef.getFieldStart(cols[colIdx]),
                                            tRef.getFieldLength(cols[colIdx]));
                                    IExternalLangIPCProto.visitValueRef(fnDescs[func].getArgumentTypes()[colIdx],
                                            argHolders.get(func).getDataOutput(), ref, pointableAllocator,
                                            pointableVisitor, fnDescs[func].getFunctionInfo().getNullCall());
                                }
                            } else {
                                numCalls[func]--;
                            }
                            if (cols.length == 0) {
                                ExternalDataUtils.setVoidArgument(argHolders.get(func));
                            }
                        }
                    }

                    //TODO: maybe this could be done in parallel for each unique library evaluator?
                    for (int argHolderIdx = 0; argHolderIdx < argHolders.size(); argHolderIdx++) {
                        Pair<Long, ILibraryEvaluator> fnEval = libraryEvaluators.get(argHolderIdx);
                        ByteBuffer columnResult = fnEval.getSecond().callMulti(fnEval.getFirst(),
                                argHolders.get(argHolderIdx), numCalls[argHolderIdx]);
                        if (columnResult != null) {
                            Pair<ByteBuffer, Counter> resultholder = batchResults.get(argHolderIdx);
                            if (resultholder.getFirst().capacity() < columnResult.remaining()) {
                                ByteBuffer realloc =
                                        ctx.reallocateFrame(resultholder.getFirst(),
                                                ctx.getInitialFrameSize()
                                                        * ((columnResult.remaining() / ctx.getInitialFrameSize()) + 1),
                                                false);
                                realloc.limit(columnResult.limit());
                                resultholder.setFirst(realloc);
                            }
                            ByteBuffer resultBuf = resultholder.getFirst();
                            //offset 1 to skip message type
                            System.arraycopy(columnResult.array(), 1, resultBuf.array(), 0,
                                    columnResult.remaining() - 1);
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
                                outputWrapper.reset();
                                Pair<ByteBuffer, Counter> result = batchResults.get(k);
                                ATypeTag functionCalled = nullCalls[k][i];
                                if (functionCalled == ATypeTag.TYPE) {
                                    if (result.getSecond().get() > 0) {
                                        unpackerToADM.unpack(result.getFirst(), outputWrapper.getDataOutput(), true);
                                        result.getSecond().set(result.getSecond().get() - 1);
                                    } else {
                                        //emit NULL for functions which failed with a warning
                                        outputWrapper.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                                    }
                                } else if (functionCalled == ATypeTag.NULL) {
                                    outputWrapper.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                                } else {
                                    outputWrapper.getDataOutput().writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                                }
                                tupleBuilder.addField(outputWrapper.getByteArray(), 0, outputWrapper.getLength());
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
