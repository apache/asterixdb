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

import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

/*
 * A single activity operator that provides the functionality of scanning data using an
 * instance of the configured adapter.
 */
public class ExternalScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 2L;

    private final ITypedAdapterFactory adapterFactory;

    private final ITupleFilterFactory tupleFilterFactory;

    private final long outputLimit;

    public ExternalScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc,
            ITypedAdapterFactory dataSourceAdapterFactory, ITupleFilterFactory tupleFilterFactory, long outputLimit) {
        super(spec, 0, 1);
        outRecDescs[0] = rDesc;
        this.adapterFactory = dataSourceAdapterFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.outputLimit = outputLimit;
    }

    public ExternalScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc,
            ITypedAdapterFactory dataSourceAdapterFactory) {
        this(spec, rDesc, dataSourceAdapterFactory, null, -1);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            private IOperatorStats stats;

            @Override
            public void initialize() throws HyracksDataException {
                IDataSourceAdapter adapter;
                if (ctx.getStatsCollector() != null) {
                    stats = ctx.getStatsCollector().getOrAddOperatorStats(getDisplayName());
                }
                try {
                    writer.open();
                    ITupleFilter tupleFilter =
                            tupleFilterFactory != null ? tupleFilterFactory.createTupleFilter(ctx) : null;
                    adapter = adapterFactory.createAdapter(ctx, partition);
                    adapter.start(partition, writer, tupleFilter, outputLimit);
                    if (stats != null) {
                        stats.getTupleCounter().update(adapter.getProcessedTuples());
                    }
                } catch (Exception e) {
                    writer.fail();
                    throw HyracksDataException.create(e);
                } finally {
                    writer.close();
                }
            }
        };

    }

}
