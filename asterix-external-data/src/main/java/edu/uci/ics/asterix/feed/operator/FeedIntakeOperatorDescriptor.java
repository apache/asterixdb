/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.feed.operator;

import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.IFeedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.IFeedDatasetAdapterFactory.FeedAdapterType;
import edu.uci.ics.asterix.external.adapter.factory.IGenericFeedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.ITypedFeedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.IFeedDatasourceAdapter;
import edu.uci.ics.asterix.feed.mgmt.FeedId;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String adapterFactoryClassName;
    private final Map<String, String> adapterConfiguration;
    private final IAType atype;
    private final FeedId feedId;

    private transient IFeedDatasetAdapterFactory datasourceAdapterFactory;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, FeedId feedId, String adapter,
            Map<String, String> arguments, ARecordType atype, RecordDescriptor rDesc) {
        super(spec, 1, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactoryClassName = adapter;
        this.adapterConfiguration = arguments;
        this.atype = atype;
        this.feedId = feedId;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        IFeedDatasourceAdapter adapter;
        try {
            datasourceAdapterFactory = (IFeedDatasetAdapterFactory) Class.forName(adapterFactoryClassName)
                    .newInstance();
            if (datasourceAdapterFactory.getFeedAdapterType().equals(FeedAdapterType.GENERIC)) {
                adapter = (IFeedDatasourceAdapter) ((IGenericFeedDatasetAdapterFactory) datasourceAdapterFactory)
                        .createAdapter(adapterConfiguration, atype);
            } else {
                adapter = (IFeedDatasourceAdapter) ((ITypedFeedDatasetAdapterFactory) datasourceAdapterFactory)
                        .createAdapter(adapterConfiguration);
            }
            adapter.initialize(ctx);
        } catch (Exception e) {
            throw new HyracksDataException("initialization of adapter failed", e);
        }
        return new FeedIntakeOperatorNodePushable(feedId, adapter, partition);
    }
}
