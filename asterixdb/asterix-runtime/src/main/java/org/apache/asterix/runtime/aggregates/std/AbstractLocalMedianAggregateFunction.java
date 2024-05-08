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

package org.apache.asterix.runtime.aggregates.std;

import java.util.List;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.partitions.JobFileState;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.ISorter;

public abstract class AbstractLocalMedianAggregateFunction extends AbstractMedianAggregateFunction {

    protected final ExternalSortRunGenerator runsGenerator;
    protected final IFrameTupleAppender appender;
    private final ArrayTupleBuilder tupleBuilder;
    private final int numFrames;
    private ExternalSortRunMerger runsMerger;

    public AbstractLocalMedianAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, int numFrames) throws HyracksDataException {
        super(args, context, sourceLoc);
        this.numFrames = numFrames;
        appender = new FrameTupleAppender(frame);
        tupleBuilder = new ArrayTupleBuilder(1);
        runsGenerator = new ExternalSortRunGenerator(context.getTaskContext(), new int[] { 0 },
                new INormalizedKeyComputerFactory[] { doubleNkComputerFactory },
                new IBinaryComparatorFactory[] { doubleComparatorFactory }, recordDesc, Algorithm.MERGE_SORT,
                EnumFreeSlotPolicy.LAST_FIT, numFrames, Integer.MAX_VALUE);

    }

    @Override
    public void init() throws HyracksDataException {
        super.init();
        appender.reset(frame, true);
        runsGenerator.open();
        runsGenerator.getSorter().reset();
    }

    protected void processDataValue(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int start = inputVal.getStartOffset();
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[start]);
        if (ATypeHierarchy.getTypeDomain(tag) == ATypeHierarchy.Domain.NUMERIC) {
            count++;
            aDouble.setValue(ATypeHierarchy.getDoubleValue(MEDIAN, 0, data, start));
            tupleBuilder.reset();
            tupleBuilder.addField(doubleSerde, aDouble);
            FrameUtils.appendToWriter(runsGenerator, appender, tupleBuilder.getFieldEndOffsets(),
                    tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
        }
    }

    protected void finishLocalPartial(IPointable result) throws HyracksDataException {
        if (count == 0) {
            setPartialResult(result, -1, "", -1);
            return;
        }
        if (appender.getTupleCount() > 0) {
            appender.write(runsGenerator, true);
        }
        // close to sort the in-memory data or write out sorted data to run files
        runsGenerator.close();

        IHyracksTaskContext taskCtx = ctx.getTaskContext();
        IHyracksJobletContext jobletCtx = taskCtx.getJobletContext();
        NetworkAddress netAddress = ((NodeControllerService) jobletCtx.getServiceContext().getControllerService())
                .getNetworkManager().getPublicNetworkAddress();
        FileReference fileRef = writeRunFile(taskCtx, jobletCtx);
        long fileId = jobletCtx.nextUniqueId();
        taskCtx.setStateObject(new JobFileState(fileRef, jobletCtx.getJobId(), fileId));
        setPartialResult(result, fileId, netAddress.getAddress(), netAddress.getPort());
    }

    private FileReference writeRunFile(IHyracksTaskContext taskCtx, IHyracksJobletContext jobletCtx)
            throws HyracksDataException {
        List<GeneratedRunFileReader> runs = runsGenerator.getRuns();
        FileReference managedFile;
        if (runs.isEmpty()) {
            managedFile = jobletCtx.createManagedWorkspaceFile(MEDIAN);
            writeMemoryDataToRunFile(managedFile, taskCtx);
        } else if (runs.size() == 1) {
            managedFile = runs.get(0).getFile();
        } else {
            managedFile = jobletCtx.createManagedWorkspaceFile(MEDIAN);
            mergeRunsToRunFile(managedFile, taskCtx, runs);
        }
        return managedFile;
    }

    private void mergeRunsToRunFile(FileReference managedFile, IHyracksTaskContext taskCtx,
            List<GeneratedRunFileReader> runs) throws HyracksDataException {
        createOrResetRunsMerger(runs);
        RunFileWriter runFileWriter = new RunFileWriter(managedFile, taskCtx.getIoManager());
        IFrameWriter wrappingWriter = runsMerger.prepareFinalMergeResultWriter(runFileWriter);
        try {
            wrappingWriter.open();
            runsMerger.process(wrappingWriter);
        } finally {
            wrappingWriter.close();
        }
    }

    protected RunFileWriter writeMemoryDataToRunFile(FileReference managedFile, IHyracksTaskContext taskCtx)
            throws HyracksDataException {
        RunFileWriter runFileWriter = new RunFileWriter(managedFile, taskCtx.getIoManager());
        try {
            runFileWriter.open();
            ISorter sorter = runsGenerator.getSorter();
            if (sorter.hasRemaining()) {
                sorter.flush(runFileWriter);
            }
        } finally {
            runFileWriter.close();
        }
        return runFileWriter;
    }

    private void createOrResetRunsMerger(List<GeneratedRunFileReader> runs) {
        if (runsMerger == null) {
            IBinaryComparator[] comparators =
                    new IBinaryComparator[] { doubleComparatorFactory.createBinaryComparator() };
            INormalizedKeyComputer nmkComputer = doubleNkComputerFactory.createNormalizedKeyComputer();
            runsMerger = new ExternalSortRunMerger(ctx.getTaskContext(), runs, new int[] { 0 }, comparators,
                    nmkComputer, recordDesc, numFrames, Integer.MAX_VALUE);
        } else {
            runsMerger.reset(runs);
        }
    }
}
