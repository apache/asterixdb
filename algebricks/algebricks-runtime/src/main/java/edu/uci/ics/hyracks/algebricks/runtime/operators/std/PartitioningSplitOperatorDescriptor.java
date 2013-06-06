/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;

public class PartitioningSplitOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    public static int NO_DEFAULT_BRANCH = -1;
    
    private final ICopyEvaluatorFactory[] evalFactories;
    private final IBinaryBooleanInspector boolInspector;
    private final int defaultBranchIndex;
    
    public PartitioningSplitOperatorDescriptor(IOperatorDescriptorRegistry spec, ICopyEvaluatorFactory[] evalFactories,
            IBinaryBooleanInspector boolInspector, int defaultBranchIndex, RecordDescriptor rDesc) {
        super(spec, 1, (defaultBranchIndex == evalFactories.length) ? evalFactories.length + 1 : evalFactories.length);
        for (int i = 0; i < evalFactories.length; i++) {
            recordDescriptors[i] = rDesc;
        }
        this.evalFactories = evalFactories;
        this.boolInspector = boolInspector;
        this.defaultBranchIndex = defaultBranchIndex;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputOperatorNodePushable() {
            private final IFrameWriter[] writers = new IFrameWriter[outputArity];
            private final ByteBuffer[] writeBuffers = new ByteBuffer[outputArity];
            private final ICopyEvaluator[] evals = new ICopyEvaluator[outputArity];
            private final ArrayBackedValueStorage evalBuf = new ArrayBackedValueStorage();
            private final RecordDescriptor inOutRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), inOutRecDesc);
            private final FrameTupleReference frameTuple = new FrameTupleReference();
            
            private final FrameTupleAppender tupleAppender = new FrameTupleAppender(ctx.getFrameSize());
            private final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(inOutRecDesc.getFieldCount());
            private final DataOutput tupleDos = tupleBuilder.getDataOutput();
            
            @Override
            public void close() throws HyracksDataException {
                // Flush (possibly not full) buffers that have data, and close writers.
                for (int i = 0; i < outputArity; i++) {
                    tupleAppender.reset(writeBuffers[i], false);
                    if (tupleAppender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(writeBuffers[i], writers[i]);
                    }
                    writers[i].close();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.fail();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    frameTuple.reset(accessor, i);
                    boolean found = false;
                    for (int j = 0; j < evals.length; j++) {
                        try {
                        	evalBuf.reset();
                            evals[j].evaluate(frameTuple);
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                        found = boolInspector.getBooleanValue(evalBuf.getByteArray(), 0, 1);
                        if (found) {
                        	copyAndAppendTuple(j);
                        	break;
                        }
                    }
                    // Optionally write to default output branch.
                    if (!found && defaultBranchIndex != NO_DEFAULT_BRANCH) {
                    	copyAndAppendTuple(defaultBranchIndex);
                    }
                }
            }

            private void copyAndAppendTuple(int outputIndex) throws HyracksDataException {
            	// Copy tuple into tuple builder.
                try {
                	tupleBuilder.reset();
                    for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                        tupleDos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                                frameTuple.getFieldLength(i));
                        tupleBuilder.addFieldEndOffset();
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                // Append to frame.
                tupleAppender.reset(writeBuffers[outputIndex], false);
                if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                    FrameUtils.flushFrame(writeBuffers[outputIndex], writers[outputIndex]);
                    tupleAppender.reset(writeBuffers[outputIndex], true);
                    if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                        throw new IllegalStateException();
                    }
                }
            }
            
            @Override
            public void open() throws HyracksDataException {
                for (IFrameWriter writer : writers) {
                    writer.open();
                }
                // Create write buffers.
                for (int i = 0; i < outputArity; i++) {
                    writeBuffers[i] = ctx.allocateFrame();
                    // Make sure to clear all buffers, since we are reusing the tupleAppender.
                    tupleAppender.reset(writeBuffers[i], true);
                }
                // Create evaluators for partitioning.
				try {
					for (int i = 0; i < evalFactories.length; i++) {
						evals[i] = evalFactories[i].createEvaluator(evalBuf);
					}
				} catch (AlgebricksException e) {
					throw new HyracksDataException(e);
				}
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                writers[index] = writer;
            }
        };
    }
}

