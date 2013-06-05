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
package edu.uci.ics.asterix.external.data.operator;

import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.IAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasetAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.ITypedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.ITypedDatasourceAdapter;
import edu.uci.ics.asterix.external.feed.lifecycle.FeedId;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * Operator responsible for ingesting data from an external source. This
 * operator uses a (configurable) adapter associated with the feed dataset.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final String adapterFactoryClassName;
    private final Map<String, Object> adapterConfiguration;
    private final IAType atype;
    private final FeedId feedId;
    private final IAdapterFactory datasourceAdapterFactory;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, FeedId feedId, String adapter,
            Map<String, Object> arguments, ARecordType atype, RecordDescriptor rDesc,
            IAdapterFactory datasourceAdapterFactory) {
        super(spec, 1, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactoryClassName = adapter;
        this.adapterConfiguration = arguments;
        this.atype = atype;
        this.feedId = feedId;
        this.datasourceAdapterFactory = datasourceAdapterFactory;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        ITypedDatasourceAdapter adapter;
        try {
            if (datasourceAdapterFactory instanceof IGenericDatasetAdapterFactory) {
                adapter = (ITypedDatasourceAdapter) ((IGenericDatasetAdapterFactory) datasourceAdapterFactory)
                        .createAdapter(adapterConfiguration, atype);
            } else if (datasourceAdapterFactory instanceof ITypedDatasetAdapterFactory) {
                adapter = (ITypedDatasourceAdapter) ((ITypedDatasetAdapterFactory) datasourceAdapterFactory)
                        .createAdapter(adapterConfiguration);
            } else {
                throw new IllegalStateException(" Unknown adapter factory type for " + adapterFactoryClassName);
            }
            adapter.initialize(ctx);
        } catch (Exception e) {
            throw new HyracksDataException("initialization of adapter failed", e);
        }
        return new FeedIntakeOperatorNodePushable(feedId, adapter, partition);
    }
}
