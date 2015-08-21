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
package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

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
