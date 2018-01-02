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

import java.util.Map;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An operator responsible for establishing connection with external data source and parsing,
 * translating the received content.It uses an instance of feed adaptor to perform these functions.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final String FEED_EXTENSION_NAME = "Feed";

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    /** The unique identifier of the feed that is being ingested. **/
    private final EntityId feedId;

    private final FeedPolicyAccessor policyAccessor;
    private final ARecordType adapterOutputType;
    /** The adaptor factory that is used to create an instance of the feed adaptor **/
    private IAdapterFactory adaptorFactory;
    /** The library that contains the adapter in use. **/
    private String adaptorLibraryName;
    /**
     * The adapter factory class that is used to create an instance of the feed adapter.
     * This value is used only in the case of external adapters.
     **/
    private String adaptorFactoryClassName;
    /** The configuration parameters associated with the adapter. **/
    private Map<String, String> adaptorConfiguration;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, IFeed primaryFeed, IAdapterFactory adapterFactory,
            ARecordType adapterOutputType, FeedPolicyAccessor policyAccessor, RecordDescriptor rDesc) {
        super(spec, 0, 1);
        this.feedId = new EntityId(FEED_EXTENSION_NAME, primaryFeed.getDataverseName(), primaryFeed.getFeedName());
        this.adaptorFactory = adapterFactory;
        this.adapterOutputType = adapterOutputType;
        this.policyAccessor = policyAccessor;
        this.outRecDescs[0] = rDesc;
    }

    public FeedIntakeOperatorDescriptor(JobSpecification spec, IFeed feed, String adapterLibraryName,
            String adapterFactoryClassName, ARecordType adapterOutputType, FeedPolicyAccessor policyAccessor,
            RecordDescriptor rDesc) {
        super(spec, 0, 1);
        this.feedId = new EntityId(FEED_EXTENSION_NAME, feed.getDataverseName(), feed.getFeedName());
        this.adaptorFactoryClassName = adapterFactoryClassName;
        this.adaptorLibraryName = adapterLibraryName;
        this.adaptorConfiguration = feed.getConfiguration();
        this.adapterOutputType = adapterOutputType;
        this.policyAccessor = policyAccessor;
        this.outRecDescs[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        if (adaptorFactory == null) {
            adaptorFactory = createExternalAdapterFactory(ctx);
        }
        return new FeedIntakeOperatorNodePushable(ctx, feedId, adaptorFactory, partition, recordDescProvider, this);
    }

    private IAdapterFactory createExternalAdapterFactory(IHyracksTaskContext ctx) throws HyracksDataException {
        IAdapterFactory adapterFactory;
        INcApplicationContext runtimeCtx =
                (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
        ILibraryManager libraryManager = runtimeCtx.getLibraryManager();
        ClassLoader classLoader = libraryManager.getLibraryClassLoader(feedId.getDataverse(), adaptorLibraryName);
        if (classLoader != null) {
            try {
                adapterFactory = (IAdapterFactory) (classLoader.loadClass(adaptorFactoryClassName).newInstance());
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.configure(ctx.getJobletContext().getServiceContext(), adaptorConfiguration);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        } else {
            RuntimeDataException err = new RuntimeDataException(
                    ErrorCode.OPERATORS_FEED_INTAKE_OPERATOR_DESCRIPTOR_CLASSLOADER_NOT_CONFIGURED, adaptorLibraryName,
                    feedId.getDataverse());
            LOGGER.error(err.getMessage());
            throw err;
        }
        return adapterFactory;
    }

    public EntityId getEntityId() {
        return feedId;
    }

    public IAdapterFactory getAdaptorFactory() {
        return this.adaptorFactory;
    }

    public void setAdaptorFactory(IAdapterFactory factory) {
        this.adaptorFactory = factory;
    }

    public ARecordType getAdapterOutputType() {
        return this.adapterOutputType;
    }

    public FeedPolicyAccessor getPolicyAccessor() {
        return this.policyAccessor;
    }

    public String getAdaptorLibraryName() {
        return this.adaptorLibraryName;
    }

    public String getAdaptorFactoryClassName() {
        return this.adaptorFactoryClassName;
    }

}
