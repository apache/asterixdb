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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
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
 * FeedIntakeOperatorDescriptor is responsible for ingesting data from an external source. This
 * operator uses a user specified for a built-in adapter for retrieving data from the external
 * data source.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FeedIntakeOperatorDescriptor.class.getName());

    /** The type associated with the ADM data output from the feed adapter */
    private final IAType outputType;

    /** unique identifier for a feed instance. */
    private final FeedConnectionId feedId;

    /** Map representation of policy parameters */
    private final Map<String, String> feedPolicy;

    /** The adapter factory that is used to create an instance of the feed adapter **/
    private IAdapterFactory adapterFactory;

    /** The (singleton) instance of IFeedManager **/
    private IFeedManager feedManager;

    /** The library that contains the adapter in use. **/
    private String adapterLibraryName;

    /**
     * The adapter factory class that is used to create an instance of the feed adapter.
     * This value is used only in the case of external adapters.
     **/
    private String adapterFactoryClassName;

    /** The configuration parameters associated with the adapter. **/
    private Map<String, String> adapterConfiguration;

    private ARecordType adapterOutputType;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, FeedConnectionId feedId, IAdapterFactory adapterFactory,
            ARecordType atype, RecordDescriptor rDesc, Map<String, String> feedPolicy) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactory = adapterFactory;
        this.outputType = atype;
        this.feedId = feedId;
        this.feedPolicy = feedPolicy;
    }

    public FeedIntakeOperatorDescriptor(JobSpecification spec, FeedConnectionId feedId, String adapterLibraryName,
            String adapterFactoryClassName, Map<String, String> configuration, ARecordType atype,
            RecordDescriptor rDesc, Map<String, String> feedPolicy) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactoryClassName = adapterFactoryClassName;
        this.adapterConfiguration = configuration;
        this.adapterLibraryName = adapterLibraryName;
        this.outputType = atype;
        this.feedId = feedId;
        this.feedPolicy = feedPolicy;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        IFeedAdapter adapter = null;
        FeedRuntimeId feedRuntimeId = new FeedRuntimeId(feedId, FeedRuntimeType.INGESTION, partition);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
        IngestionRuntime ingestionRuntime = (IngestionRuntime) feedManager.getFeedRuntime(feedRuntimeId);
        try {
            if (ingestionRuntime == null) {
                // create an instance of a feed adapter to ingest data.
                adapter = createAdapter(ctx, partition);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning new feed:" + feedId);
                }
            } else {
                // retrieve the instance of the feed adapter used in previous failed execution.
                adapter = ((IngestionRuntime) ingestionRuntime).getAdapterRuntimeManager().getFeedAdapter();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Resuming old feed:" + feedId);
                }
            }
        } catch (Exception exception) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Initialization of the feed adapter failed with exception " + exception);
            }
            throw new HyracksDataException("Initialization of the feed adapter failed", exception);
        }
        return new FeedIntakeOperatorNodePushable(ctx, feedId, adapter, feedPolicy, partition, ingestionRuntime);
    }

    public FeedConnectionId getFeedId() {
        return feedId;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public IAdapterFactory getAdapterFactory() {
        return adapterFactory;
    }

    public IAType getOutputType() {
        return outputType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    private IFeedAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IFeedAdapter feedAdapter = null;
        if (adapterFactory != null) {
            feedAdapter = (IFeedAdapter) adapterFactory.createAdapter(ctx, partition);
        } else {
            ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(feedId.getDataverse(),
                    adapterLibraryName);
            if (classLoader != null) {
                IAdapterFactory adapterFactory = ((IAdapterFactory) (classLoader.loadClass(adapterFactoryClassName)
                        .newInstance()));

                switch (adapterFactory.getAdapterType()) {
                    case TYPED: {
                        ((ITypedAdapterFactory) adapterFactory).configure(adapterConfiguration);
                        feedAdapter = (IFeedAdapter) ((ITypedAdapterFactory) adapterFactory).createAdapter(ctx,
                                partition);
                    }
                        break;
                    case GENERIC: {
                        String outputTypeName = adapterConfiguration.get(IGenericAdapterFactory.KEY_TYPE_NAME);
                        if (outputTypeName == null) {
                            throw new IllegalArgumentException(
                                    "You must specify the datatype associated with the incoming data. Datatype is specified by the "
                                            + IGenericAdapterFactory.KEY_TYPE_NAME + " configuration parameter");
                        }
                        ((IGenericAdapterFactory) adapterFactory).configure(adapterConfiguration,
                                (ARecordType) adapterOutputType, false, null);
                        ((IGenericAdapterFactory) adapterFactory).createAdapter(ctx, partition);
                    }
                        break;
                }

                feedAdapter = (IFeedAdapter) adapterFactory.createAdapter(ctx, partition);
            } else {
                String message = "Unable to create adapter as class loader not configured for library "
                        + adapterLibraryName + " in dataverse " + feedId.getDataverse();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(message);
                }
                throw new IllegalArgumentException(message);

            }
        }
        return feedAdapter;
    }

    public String getAdapterLibraryName() {
        return adapterLibraryName;
    }

    public String getAdapterFactoryClassName() {
        return adapterFactoryClassName;
    }

    public Map<String, String> getAdapterConfiguration() {
        return adapterConfiguration;
    }
}
