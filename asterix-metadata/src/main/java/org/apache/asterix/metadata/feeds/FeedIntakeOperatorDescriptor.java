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

import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.IngestionRuntime;
import org.apache.asterix.common.feeds.SubscribableFeedRuntimeId;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;
import org.apache.asterix.metadata.entities.PrimaryFeed;
import org.apache.asterix.metadata.functions.ExternalLibraryManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * An operator responsible for establishing connection with external data source and parsing,
 * translating the received content.It uses an instance of feed adaptor to perform these functions.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedIntakeOperatorDescriptor.class.getName());

    /** The unique identifier of the feed that is being ingested. **/
    private final FeedId feedId;

    private final FeedPolicyAccessor policyAccessor;

    /** The adaptor factory that is used to create an instance of the feed adaptor **/
    private IFeedAdapterFactory adaptorFactory;

    /** The library that contains the adapter in use. **/
    private String adaptorLibraryName;

    /**
     * The adapter factory class that is used to create an instance of the feed adapter.
     * This value is used only in the case of external adapters.
     **/
    private String adaptorFactoryClassName;

    /** The configuration parameters associated with the adapter. **/
    private Map<String, String> adaptorConfiguration;

    private ARecordType adapterOutputType;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, PrimaryFeed primaryFeed,
            IFeedAdapterFactory adapterFactory, ARecordType adapterOutputType, FeedPolicyAccessor policyAccessor) {
        super(spec, 0, 1);
        this.feedId = new FeedId(primaryFeed.getDataverseName(), primaryFeed.getFeedName());
        this.adaptorFactory = adapterFactory;
        this.adapterOutputType = adapterOutputType;
        this.policyAccessor = policyAccessor;
    }

    public FeedIntakeOperatorDescriptor(JobSpecification spec, PrimaryFeed primaryFeed, String adapterLibraryName,
            String adapterFactoryClassName, ARecordType adapterOutputType, FeedPolicyAccessor policyAccessor) {
        super(spec, 0, 1);
        this.feedId = new FeedId(primaryFeed.getDataverseName(), primaryFeed.getFeedName());
        this.adaptorFactoryClassName = adapterFactoryClassName;
        this.adaptorLibraryName = adapterLibraryName;
        this.adaptorConfiguration = primaryFeed.getAdaptorConfiguration();
        this.adapterOutputType = adapterOutputType;
        this.policyAccessor = policyAccessor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        IFeedSubscriptionManager feedSubscriptionManager = runtimeCtx.getFeedManager().getFeedSubscriptionManager();
        SubscribableFeedRuntimeId feedIngestionId = new SubscribableFeedRuntimeId(feedId, FeedRuntimeType.INTAKE,
                partition);
        IngestionRuntime ingestionRuntime = (IngestionRuntime) feedSubscriptionManager
                .getSubscribableRuntime(feedIngestionId);
        if (adaptorFactory == null) {
            try {
                adaptorFactory = createExtenralAdapterFactory(ctx, partition);
            } catch (Exception exception) {
                throw new HyracksDataException(exception);
            }

        }
        return new FeedIntakeOperatorNodePushable(ctx, feedId, adaptorFactory, partition, ingestionRuntime,
                policyAccessor);
    }

    private IFeedAdapterFactory createExtenralAdapterFactory(IHyracksTaskContext ctx, int partition) throws Exception {
        IFeedAdapterFactory adapterFactory = null;
        ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(feedId.getDataverse(),
                adaptorLibraryName);
        if (classLoader != null) {
            adapterFactory = ((IFeedAdapterFactory) (classLoader.loadClass(adaptorFactoryClassName).newInstance()));
            adapterFactory.configure(adaptorConfiguration, adapterOutputType);
        } else {
            String message = "Unable to create adapter as class loader not configured for library "
                    + adaptorLibraryName + " in dataverse " + feedId.getDataverse();
            LOGGER.severe(message);
            throw new IllegalArgumentException(message);
        }
        return adapterFactory;
    }

    public FeedId getFeedId() {
        return feedId;
    }

}
