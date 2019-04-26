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
package org.apache.hyracks.dataflow.std.sort;

import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

public class ExternalSortRunMerger extends AbstractExternalSortRunMerger {

    private final int[] sortFields;

    public ExternalSortRunMerger(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs, int[] sortFields,
            IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, RecordDescriptor recordDesc,
            int framesLimit, int topK) {
        super(ctx, runs, comparators, nmkComputer, recordDesc, framesLimit, topK);
        this.sortFields = sortFields;
    }

    @Override
    public IFrameWriter prepareSkipMergingFinalResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        return nextWriter;
    }

    @Override
    protected RunFileWriter prepareIntermediateMergeRunFile() throws HyracksDataException {
        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortRunMerger.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIoManager());
    }

    @Override
    protected IFrameWriter prepareIntermediateMergeResultWriter(RunFileWriter mergeFileWriter)
            throws HyracksDataException {
        return mergeFileWriter;
    }

    @Override
    public IFrameWriter prepareFinalMergeResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        return nextWriter;
    }

    @Override
    protected int[] getSortFields() {
        return sortFields;
    }

}
