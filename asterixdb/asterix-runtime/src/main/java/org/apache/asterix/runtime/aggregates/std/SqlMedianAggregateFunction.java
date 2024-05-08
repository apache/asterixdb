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

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.sort.RunMergingFrameReader;

public class SqlMedianAggregateFunction extends AbstractLocalMedianAggregateFunction {

    public SqlMedianAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, int numFrames) throws HyracksDataException {
        super(args, context, sourceLoc, numFrames);
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        processDataValue(tuple);
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            appender.write(runsGenerator, true);
        }
        // close to sort the in-memory data or write out sorted data to run files
        runsGenerator.close();
        super.finishFinalResult(result);
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        finishLocalPartial(result);
    }

    @Override
    protected RunMergingFrameReader createRunsMergingFrameReader() throws HyracksDataException {
        IHyracksTaskContext taskCtx = ctx.getTaskContext();
        List<GeneratedRunFileReader> runs = runsGenerator.getRuns();
        readers.clear();
        if (runs.isEmpty()) {
            //TODO: no need to write memory to run file, should just read the sorted data out of the sorter
            FileReference managedFile = taskCtx.createManagedWorkspaceFile(MEDIAN);
            RunFileWriter runFileWriter = writeMemoryDataToRunFile(managedFile, taskCtx);
            GeneratedRunFileReader deleteOnCloseReader = runFileWriter.createDeleteOnCloseReader();
            readers.add(deleteOnCloseReader);
        } else {
            readers.addAll(runs);
        }

        List<IFrame> inFrames = getInFrames(readers.size(), taskCtx);
        return new RunMergingFrameReader(taskCtx, readers, inFrames, new int[] { 0 },
                new IBinaryComparator[] { doubleComparatorFactory.createBinaryComparator() },
                doubleNkComputerFactory.createNormalizedKeyComputer(), recordDesc);
    }
}