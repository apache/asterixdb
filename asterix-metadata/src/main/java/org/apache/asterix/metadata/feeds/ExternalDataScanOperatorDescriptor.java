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
package org.apache.asterix.metadata.feeds;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.metadata.external.IAdapterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/*
 * A single activity operator that provides the functionality of scanning data using an
 * instance of the configured adapter.
 */
public class ExternalDataScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private IAdapterFactory adapterFactory;
    

    public ExternalDataScanOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc,
            IAdapterFactory dataSourceAdapterFactory) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactory = dataSourceAdapterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {

        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                writer.open();
                IDatasourceAdapter adapter = null;
                try {
                    adapter = adapterFactory.createAdapter(ctx, partition);
                    adapter.start(partition, writer);
                } catch (Exception e) {
                    throw new HyracksDataException("exception during reading from external data source", e);
                } finally {
                    writer.close();
                }
            }
        };

    }

}
