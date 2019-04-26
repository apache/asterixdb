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

package org.apache.hyracks.tests.unit;

import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.sort.ExternalSortGroupByRunGenerator;
import org.apache.hyracks.dataflow.std.group.sort.ExternalSortGroupByRunMerger;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ISorter;

public class SortGroupbyTest extends AbstractExternalGroupbyTest {
    ExternalSortGroupByRunGenerator builder;

    IOperatorNodePushable mergerOperator;

    @Override
    protected void initial(final IHyracksTaskContext ctx, int tableSize, final int numFrames)
            throws HyracksDataException {
        builder = new ExternalSortGroupByRunGenerator(ctx, keyFields, inRecordDesc, numFrames, keyFields,
                normalizedKeyComputerFactory, comparatorFactories, partialAggrInState, outputRec, Algorithm.QUICK_SORT);

        mergerOperator = new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                List<GeneratedRunFileReader> runs = builder.getRuns();
                ISorter sorter = builder.getSorter();
                IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                for (int i = 0; i < comparatorFactories.length; ++i) {
                    comparators[i] = comparatorFactories[i].createBinaryComparator();
                }
                INormalizedKeyComputer nmkComputer = normalizedKeyComputerFactory == null ? null
                        : normalizedKeyComputerFactory.createNormalizedKeyComputer();
                AbstractExternalSortRunMerger merger = new ExternalSortGroupByRunMerger(ctx, runs, keyFields,
                        inRecordDesc, outputRec, outputRec, numFrames, keyFields, nmkComputer, comparators,
                        partialAggrInState, finalAggrInState, true);
                IFrameWriter wrappingWriter = null;
                try {
                    if (runs.isEmpty()) {
                        wrappingWriter = merger.prepareSkipMergingFinalResultWriter(writer);
                        wrappingWriter.open();
                        if (sorter.hasRemaining()) {
                            sorter.flush(wrappingWriter);
                        }
                    } else {
                        wrappingWriter = merger.prepareFinalMergeResultWriter(writer);
                        wrappingWriter.open();
                        merger.process(wrappingWriter);
                    }
                } catch (Throwable e) {
                    if (wrappingWriter != null) {
                        wrappingWriter.fail();
                    }
                    throw HyracksDataException.create(e);
                } finally {
                    sorter.close();
                    if (wrappingWriter != null) {
                        wrappingWriter.close();
                    }
                }
            }
        };
    }

    @Override
    protected IFrameWriter getBuilder() {
        return builder;
    }

    @Override
    protected IOperatorNodePushable getMerger() {
        return mergerOperator;
    }
}
