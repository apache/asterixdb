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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IDataSourceAdapter.AdapterType;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningWithMessageConnectorDescriptor;

/**
 * A utility class for providing helper functions for feeds
 * TODO: Refactor this class.
 */
public class FeedMetadataUtil {

    private static final Logger LOGGER = Logger.getLogger(FeedMetadataUtil.class.getName());

    public static Dataset validateIfDatasetExists(String dataverse, String datasetName, MetadataTransactionContext ctx)
            throws AsterixException {
        Dataset dataset = MetadataManager.INSTANCE.getDataset(ctx, dataverse, datasetName);
        if (dataset == null) {
            throw new AsterixException("Unknown target dataset :" + datasetName);
        }

        if (!dataset.getDatasetType().equals(DatasetType.INTERNAL)) {
            throw new AsterixException("Statement not applicable. Dataset " + datasetName + " is not of required type "
                    + DatasetType.INTERNAL);
        }
        return dataset;
    }

    public static Feed validateIfFeedExists(String dataverse, String feedName, MetadataTransactionContext ctx)
            throws AsterixException {
        Feed feed = MetadataManager.INSTANCE.getFeed(ctx, dataverse, feedName);
        if (feed == null) {
            throw new AsterixException("Unknown source feed: " + feedName);
        }
        return feed;
    }

    public static FeedPolicyEntity validateIfPolicyExists(String dataverse, String policyName,
            MetadataTransactionContext ctx) throws AsterixException {
        FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, dataverse, policyName);
        if (feedPolicy == null) {
            feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    policyName);
            if (feedPolicy == null) {
                throw new AsterixException("Unknown feed policy" + policyName);
            }
        }
        return feedPolicy;
    }

    public static JobSpecification alterJobSpecificationForFeed(JobSpecification spec,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicyProperties) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Original Job Spec:" + spec);
        }

        JobSpecification altered = new JobSpecification(spec.getFrameSize());
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();
        boolean preProcessingRequired = preProcessingRequired(feedConnectionId);
        // copy operators
        Map<OperatorDescriptorId, OperatorDescriptorId> oldNewOID = new HashMap<>();
        FeedMetaOperatorDescriptor metaOp;
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorMap.entrySet()) {
            String operandId = null;
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                FeedCollectOperatorDescriptor orig = (FeedCollectOperatorDescriptor) opDesc;
                FeedCollectOperatorDescriptor fiop = new FeedCollectOperatorDescriptor(altered,
                        orig.getFeedConnectionId(), orig.getSourceFeedId(), (ARecordType) orig.getOutputType(),
                        orig.getRecordDescriptor(), orig.getFeedPolicyProperties(), orig.getSubscriptionLocation());
                oldNewOID.put(opDesc.getOperatorId(), fiop.getOperatorId());
            } else if ((opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor)
                    && ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).isPrimary()) {
                // only introduce store before primary index
                operandId = ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        FeedRuntimeType.STORE, false, operandId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            } else {
                FeedRuntimeType runtimeType;
                boolean enableSubscriptionMode;
                OperatorDescriptorId opId = null;
                if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                    IPushRuntimeFactory[] runtimeFactories = ((AlgebricksMetaOperatorDescriptor) opDesc).getPipeline()
                            .getRuntimeFactories();
                    if (runtimeFactories[0] instanceof AssignRuntimeFactory && runtimeFactories.length > 1) {
                        IConnectorDescriptor connectorDesc = spec.getOperatorInputMap().get(opDesc.getOperatorId())
                                .get(0);
                        IOperatorDescriptor sourceOp = spec.getProducer(connectorDesc);
                        if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                            runtimeType = FeedRuntimeType.COMPUTE;
                            enableSubscriptionMode = preProcessingRequired;
                            metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc,
                                    feedPolicyProperties, runtimeType, enableSubscriptionMode, operandId);
                            opId = metaOp.getOperatorId();
                        }
                    }
                }
                if (opId == null) {
                    opId = altered.createOperatorDescriptorId(opDesc);
                }
                oldNewOID.put(opDesc.getOperatorId(), opId);
            }
        }

        // copy connectors
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorMapping = new HashMap<>();
        for (Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : spec.getConnectorMap().entrySet()) {
            IConnectorDescriptor connDesc = entry.getValue();
            ConnectorDescriptorId newConnId;
            if (connDesc instanceof MToNPartitioningConnectorDescriptor) {
                MToNPartitioningConnectorDescriptor m2nConn = (MToNPartitioningConnectorDescriptor) connDesc;
                connDesc = new MToNPartitioningWithMessageConnectorDescriptor(altered,
                        m2nConn.getTuplePartitionComputerFactory());
                newConnId = connDesc.getConnectorId();
            } else {
                newConnId = altered.createConnectorDescriptor(connDesc);
            }
            connectorMapping.put(entry.getKey(), newConnId);
        }

        // make connections between operators
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>,
                Pair<IOperatorDescriptor, Integer>>> entry : spec.getConnectorOperatorMap().entrySet()) {
            IConnectorDescriptor connDesc = altered.getConnectorMap().get(connectorMapping.get(entry.getKey()));
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();

            IOperatorDescriptor leftOpDesc = altered.getOperatorMap()
                    .get(oldNewOID.get(leftOp.getLeft().getOperatorId()));
            IOperatorDescriptor rightOpDesc = altered.getOperatorMap()
                    .get(oldNewOID.get(rightOp.getLeft().getOperatorId()));

            altered.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
        }

        // prepare for setting partition constraints
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<>();

        for (Constraint constraint : spec.getUserConstraints()) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    operatorCounts.put(opId, (int) ((ConstantExpression) cexpr).getValue());
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();

                    IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(opId));
                    List<LocationConstraint> locations = operatorLocations.get(opDesc.getOperatorId());
                    if (locations == null) {
                        locations = new ArrayList<>();
                        operatorLocations.put(opDesc.getOperatorId(), locations);
                    }
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    LocationConstraint lc = new LocationConstraint(location,
                            ((PartitionLocationExpression) lexpr).getPartition());
                    locations.add(lc);
                    break;
                default:
                    break;
            }
        }

        // set absolute location constraints
        for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            Collections.sort(entry.getValue(), (LocationConstraint o1, LocationConstraint o2) -> {
                return o1.partition - o2.partition;
            });
            String[] locations = new String[entry.getValue().size()];
            for (int i = 0; i < locations.length; ++i) {
                locations[i] = entry.getValue().get(i).location;
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(altered, opDesc, locations);
        }

        // set count constraints
        for (Entry<OperatorDescriptorId, Integer> entry : operatorCounts.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            if (!operatorLocations.keySet().contains(entry.getKey())) {
                PartitionConstraintHelper.addPartitionCountConstraint(altered, opDesc, entry.getValue());
            }
        }

        // useConnectorSchedulingPolicy
        altered.setUseConnectorPolicyForScheduling(spec.isUseConnectorPolicyForScheduling());

        // connectorAssignmentPolicy
        altered.setConnectorPolicyAssignmentPolicy(spec.getConnectorPolicyAssignmentPolicy());

        // roots
        for (OperatorDescriptorId root : spec.getRoots()) {
            altered.addRoot(altered.getOperatorMap().get(oldNewOID.get(root)));
        }

        // jobEventListenerFactory
        altered.setJobletEventListenerFactory(spec.getJobletEventListenerFactory());

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("New Job Spec:" + altered);
        }

        return altered;

    }

    private static boolean preProcessingRequired(FeedConnectionId connectionId) {
        MetadataTransactionContext ctx = null;
        Feed feed = null;
        boolean preProcessingRequired = false;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            feed = MetadataManager.INSTANCE.getFeed(ctx, connectionId.getFeedId().getDataverse(),
                    connectionId.getFeedId().getEntityName());
            preProcessingRequired = feed.getAppliedFunction() != null;
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception abortException) {
                    e.addSuppressed(abortException);
                    throw new IllegalStateException(e);
                }
            }
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        return preProcessingRequired;
    }

    public static void validateFeed(Feed feed, MetadataTransactionContext mdTxnCtx, ILibraryManager libraryManager)
            throws AsterixException {
        try {
            String adapterName = feed.getAdapterName();
            Map<String, String> configuration = feed.getAdapterConfiguration();
            ARecordType adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            ARecordType metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            DatasourceAdapter adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx,
                    MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            AdapterType adapterType;
            IAdapterFactory adapterFactory;
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                String adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = libraryManager.getLibraryClassLoader(feed.getDataverseName(), libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.setMetaType(metaType);
                adapterFactory.configure(null, configuration);
            } else {
                AdapterFactoryProvider.getAdapterFactory(libraryManager, adapterName, configuration, adapterOutputType,
                        metaType);
            }
            if (metaType == null && configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME)) {
                metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
                if (metaType == null) {
                    throw new AsterixException("Unknown specified feed meta output data type "
                            + configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
                }
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
                if (adapterOutputType == null) {
                    throw new AsterixException("Unknown specified feed output data type "
                            + configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
                }
            }
        } catch (Exception e) {
            throw new AsterixException("Invalid feed parameters", e);
        }
    }

    @SuppressWarnings("rawtypes")
    public static Triple<IAdapterFactory, RecordDescriptor, AdapterType> getPrimaryFeedFactoryAndOutput(Feed feed,
            FeedPolicyAccessor policyAccessor, MetadataTransactionContext mdTxnCtx, ILibraryManager libraryManager)
            throws AlgebricksException {
        // This method needs to be re-visited
        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        IAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        ARecordType metaType = null;
        Triple<IAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> feedProps = null;
        IDataSourceAdapter.AdapterType adapterType = null;
        try {
            adapterName = feed.getAdapterName();
            Map<String, String> configuration = feed.getAdapterConfiguration();
            configuration.putAll(policyAccessor.getFeedPolicy());
            adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = libraryManager.getLibraryClassLoader(feed.getDataverseName(), libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.setMetaType(metaType);
                adapterFactory.configure(null, configuration);
            } else {
                adapterFactory = AdapterFactoryProvider.getAdapterFactory(libraryManager, adapterName, configuration,
                        adapterOutputType, metaType);
                adapterType = IDataSourceAdapter.AdapterType.INTERNAL;
            }
            if (metaType == null) {
                metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            }
            int numOfOutputs = 1;
            if (metaType != null) {
                numOfOutputs++;
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                // get number of PKs
                numOfOutputs += ExternalDataUtils.getNumberOfKeys(configuration);
            }
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[numOfOutputs];
            int i = 0;
            serdes[i++] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(adapterOutputType);
            if (metaType != null) {
                serdes[i++] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                getSerdesForPKs(serdes, configuration, metaType, adapterOutputType, i);
            }
            feedProps = new Triple<>(adapterFactory, new RecordDescriptor(serdes), adapterType);
        } catch (Exception e) {
            throw new AlgebricksException("unable to create adapter", e);
        }
        return feedProps;
    }

    @SuppressWarnings("rawtypes")
    private static void getSerdesForPKs(ISerializerDeserializer[] serdes, Map<String, String> configuration,
            ARecordType metaType, ARecordType adapterOutputType, int index) throws AlgebricksException {
        int[] pkIndexes = ExternalDataUtils.getPKIndexes(configuration);
        if (metaType != null) {
            int[] pkIndicators = ExternalDataUtils.getPKSourceIndicators(configuration);
            for (int j = 0; j < pkIndexes.length; j++) {
                int aInt = pkIndexes[j];
                if (pkIndicators[j] == 0) {
                    serdes[index++] = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
                } else if (pkIndicators[j] == 1) {
                    serdes[index++] = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(metaType.getFieldTypes()[aInt]);
                } else {
                    throw new AlgebricksException("a key source indicator can only be 0 or 1");
                }
            }
        } else {
            for (int aInt : pkIndexes) {
                serdes[index++] = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
            }
        }
    }

    public static ARecordType getOutputType(IFeed feed, Map<String, String> configuration, String key)
            throws RemoteException, ACIDException, MetadataException {
        ARecordType outputType = null;
        String fqOutputType = configuration.get(key);

        if (fqOutputType == null) {
            return null;
        }
        String[] dataverseAndType = fqOutputType.split("[.]");
        String dataverseName;
        String datatypeName;

        if (dataverseAndType.length == 1) {
            datatypeName = dataverseAndType[0];
            dataverseName = feed.getDataverseName();
        } else if (dataverseAndType.length == 2) {
            dataverseName = dataverseAndType[0];
            datatypeName = dataverseAndType[1];
        } else {
            throw new IllegalArgumentException("Invalid value for the parameter " + key);
        }

        MetadataTransactionContext ctx = null;
        MetadataManager.INSTANCE.acquireReadLatch();
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, dataverseName, datatypeName);
            IAType type = t.getDatatype();
            if (type.getTypeTag() != ATypeTag.RECORD) {
                throw new IllegalStateException();
            }
            outputType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (ACIDException | RemoteException | MetadataException e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (ACIDException | RemoteException e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        return outputType;
    }

    public static String getSecondaryFeedOutput(Feed feed, FeedPolicyAccessor policyAccessor,
            MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException, MetadataException, RemoteException, ACIDException {
        String outputType = null;
        String primaryFeedName = feed.getSourceFeedName();
        Feed primaryFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, feed.getDataverseName(), primaryFeedName);
        FunctionSignature appliedFunction = primaryFeed.getAppliedFunction();
        if (appliedFunction == null) {
            outputType = getOutputType(feed, feed.getAdapterConfiguration(), ExternalDataConstants.KEY_TYPE_NAME)
                    .getDisplayName();
        } else {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
            if (function != null) {
                if (function.getLanguage().equals(Function.LANGUAGE_AQL)) {
                    throw new NotImplementedException(
                            "Secondary feeds derived from a source feed that has an applied AQL function"
                                    + " are not supported yet.");
                } else {
                    outputType = function.getReturnType();
                }
            } else {
                throw new IllegalArgumentException(
                        "Function " + appliedFunction + " associated with source feed not found in Metadata.");
            }
        }
        return outputType;
    }
}
