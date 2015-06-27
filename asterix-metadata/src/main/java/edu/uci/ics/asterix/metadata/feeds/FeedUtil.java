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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.entities.SecondaryFeed;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RandomPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;

/**
 * A utility class for providing helper functions for feeds
 */
public class FeedUtil {

    private static Logger LOGGER = Logger.getLogger(FeedUtil.class.getName());

    public static String getFeedPointKeyRep(Feed feed, List<String> appliedFunctions) {
        StringBuilder builder = new StringBuilder();
        builder.append(feed.getDataverseName() + ":");
        builder.append(feed.getFeedName() + ":");
        if (appliedFunctions != null && !appliedFunctions.isEmpty()) {
            for (String function : appliedFunctions) {
                builder.append(function + ":");
            }
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static class LocationConstraint {
        int partition;
        String location;
    }

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
            throws MetadataException, AsterixException {
        Feed feed = MetadataManager.INSTANCE.getFeed(ctx, dataverse, feedName);
        if (feed == null) {
            throw new AsterixException("Unknown source feed: " + feedName);
        }
        return feed;
    }

    public static FeedPolicy validateIfPolicyExists(String dataverse, String policyName, MetadataTransactionContext ctx)
            throws AsterixException {
        FeedPolicy feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, dataverse, policyName);
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
        String operandId = null;
        Map<OperatorDescriptorId, OperatorDescriptorId> oldNewOID = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();
        FeedMetaOperatorDescriptor metaOp = null;
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorMap.entrySet()) {
            operandId = FeedRuntimeId.DEFAULT_OPERAND_ID;
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                FeedCollectOperatorDescriptor orig = (FeedCollectOperatorDescriptor) opDesc;
                FeedCollectOperatorDescriptor fiop = new FeedCollectOperatorDescriptor(altered,
                        orig.getFeedConnectionId(), orig.getSourceFeedId(), (ARecordType) orig.getOutputType(),
                        orig.getRecordDescriptor(), orig.getFeedPolicyProperties(), orig.getSubscriptionLocation());
                oldNewOID.put(opDesc.getOperatorId(), fiop.getOperatorId());
            } else if (opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor) {
                operandId = ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        FeedRuntimeType.STORE, false, operandId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            } else if (opDesc instanceof AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) {
                operandId = ((AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                        FeedRuntimeType.STORE, false, operandId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());

            } else {
                FeedRuntimeType runtimeType = null;
                boolean enableSubscriptionMode = false;
                boolean createMetaOp = true;
                OperatorDescriptorId opId = null;
                if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                    IPushRuntimeFactory runtimeFactory = ((AlgebricksMetaOperatorDescriptor) opDesc).getPipeline()
                            .getRuntimeFactories()[0];
                    if (runtimeFactory instanceof AssignRuntimeFactory) {
                        IConnectorDescriptor connectorDesc = spec.getOperatorInputMap().get(opDesc.getOperatorId())
                                .get(0);
                        IOperatorDescriptor sourceOp = spec.getProducer(connectorDesc);
                        if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                            runtimeType = preProcessingRequired ? FeedRuntimeType.COMPUTE : FeedRuntimeType.OTHER;
                            enableSubscriptionMode = preProcessingRequired;
                        } else {
                            runtimeType = FeedRuntimeType.OTHER;
                        }
                    } else if (runtimeFactory instanceof EmptyTupleSourceRuntimeFactory) {
                        runtimeType = FeedRuntimeType.ETS;
                    } else {
                        runtimeType = FeedRuntimeType.OTHER;
                    }
                } else {
                    if (opDesc instanceof AbstractSingleActivityOperatorDescriptor) {
                        runtimeType = FeedRuntimeType.OTHER;
                    } else {
                        opId = altered.createOperatorDescriptorId(opDesc);
                        createMetaOp = false;
                    }
                }
                if (createMetaOp) {
                    metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc, feedPolicyProperties,
                            runtimeType, enableSubscriptionMode, operandId);
                    opId = metaOp.getOperatorId();
                }
                oldNewOID.put(opDesc.getOperatorId(), opId);
            }
        }

        // copy connectors
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorMapping = new HashMap<ConnectorDescriptorId, ConnectorDescriptorId>();
        for (Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : spec.getConnectorMap().entrySet()) {
            IConnectorDescriptor connDesc = entry.getValue();
            ConnectorDescriptorId newConnId = altered.createConnectorDescriptor(connDesc);
            connectorMapping.put(entry.getKey(), newConnId);
        }

        // make connections between operators
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : spec
                .getConnectorOperatorMap().entrySet()) {
            IConnectorDescriptor connDesc = altered.getConnectorMap().get(connectorMapping.get(entry.getKey()));
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();

            IOperatorDescriptor leftOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(leftOp.getLeft().getOperatorId()));
            IOperatorDescriptor rightOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(rightOp.getLeft().getOperatorId()));

            altered.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
        }

        // prepare for setting partition constraints
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<OperatorDescriptorId, List<LocationConstraint>>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<OperatorDescriptorId, Integer>();

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
                    LocationConstraint lc = new LocationConstraint();
                    lc.location = location;
                    lc.partition = ((PartitionLocationExpression) lexpr).getPartition();
                    locations.add(lc);
                    break;
            }
        }

        // set absolute location constraints
        for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            Collections.sort(entry.getValue(), new Comparator<LocationConstraint>() {

                @Override
                public int compare(LocationConstraint o1, LocationConstraint o2) {
                    return o1.partition - o2.partition;
                }
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

    public static void increaseCardinality(JobSpecification spec, FeedRuntimeType compute, int requiredCardinality,
            List<String> newLocations) throws AsterixException {
        IOperatorDescriptor changingOpDesc = alterJobSpecForComputeCardinality(spec, requiredCardinality);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, changingOpDesc,
                nChooseK(requiredCardinality, newLocations));

    }

    public static void decreaseComputeCardinality(JobSpecification spec, FeedRuntimeType compute,
            int requiredCardinality, List<String> currentLocations) throws AsterixException {
        IOperatorDescriptor changingOpDesc = alterJobSpecForComputeCardinality(spec, requiredCardinality);
        String[] chosenLocations = nChooseK(requiredCardinality, currentLocations);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, changingOpDesc, chosenLocations);
    }

    private static IOperatorDescriptor alterJobSpecForComputeCardinality(JobSpecification spec, int requiredCardinality)
            throws AsterixException {
        Map<ConnectorDescriptorId, IConnectorDescriptor> connectors = spec.getConnectorMap();
        Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> connectorOpMap = spec
                .getConnectorOperatorMap();

        IOperatorDescriptor sourceOp = null;
        IOperatorDescriptor targetOp = null;
        IConnectorDescriptor connDesc = null;
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : connectorOpMap
                .entrySet()) {
            ConnectorDescriptorId cid = entry.getKey();
            sourceOp = entry.getValue().getKey().getKey();
            if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                targetOp = entry.getValue().getValue().getKey();
                if (targetOp instanceof FeedMetaOperatorDescriptor
                        && (((FeedMetaOperatorDescriptor) targetOp).getRuntimeType().equals(FeedRuntimeType.COMPUTE))) {
                    connDesc = connectors.get(cid);
                    break;
                } else {
                    throw new AsterixException("Incorrect manipulation, feed does not have a compute stage");
                }
            }
        }

        Map<OperatorDescriptorId, List<IConnectorDescriptor>> operatorInputMap = spec.getOperatorInputMap();
        boolean removed = operatorInputMap.get(targetOp.getOperatorId()).remove(connDesc);
        if (!removed) {
            throw new AsterixException("Connector desc not found");
        }
        Map<OperatorDescriptorId, List<IConnectorDescriptor>> operatorOutputMap = spec.getOperatorOutputMap();
        removed = operatorOutputMap.get(sourceOp.getOperatorId()).remove(connDesc);
        if (!removed) {
            throw new AsterixException("Connector desc not found");
        }
        spec.getConnectorMap().remove(connDesc.getConnectorId());
        connectorOpMap.remove(connDesc.getConnectorId());

        ITuplePartitionComputerFactory tpcf = new RandomPartitionComputerFactory(requiredCardinality);
        MToNPartitioningConnectorDescriptor newConnector = new MToNPartitioningConnectorDescriptor(spec, tpcf);
        spec.getConnectorMap().put(newConnector.getConnectorId(), newConnector);
        spec.connect(newConnector, sourceOp, 0, targetOp, 0);

        // ==============================================================================
        Set<Constraint> userConstraints = spec.getUserConstraints();
        Constraint countConstraint = null;
        Constraint locationConstraint = null;
        List<LocationConstraint> locations = new ArrayList<LocationConstraint>();
        IOperatorDescriptor changingOpDesc = null;

        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT: {
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    IOperatorDescriptor opDesc = spec.getOperatorMap().get(opId);
                    if (opDesc instanceof FeedMetaOperatorDescriptor) {
                        FeedRuntimeType runtimeType = ((FeedMetaOperatorDescriptor) opDesc).getRuntimeType();
                        if (runtimeType.equals(FeedRuntimeType.COMPUTE)) {
                            countConstraint = constraint;
                            changingOpDesc = opDesc;
                        }
                    }
                    break;
                }
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    IOperatorDescriptor opDesc = spec.getOperatorMap().get(opId);
                    if (opDesc instanceof FeedMetaOperatorDescriptor) {
                        FeedRuntimeType runtimeType = ((FeedMetaOperatorDescriptor) opDesc).getRuntimeType();
                        if (runtimeType.equals(FeedRuntimeType.COMPUTE)) {
                            locationConstraint = constraint;
                            changingOpDesc = opDesc;
                            String location = (String) ((ConstantExpression) cexpr).getValue();
                            LocationConstraint lc = new LocationConstraint();
                            lc.location = location;
                            lc.partition = ((PartitionLocationExpression) lexpr).getPartition();
                            locations.add(lc);
                        }
                    }

                    break;
            }
        }

        userConstraints.remove(countConstraint);
        if (locationConstraint != null) {
            userConstraints.remove(locationConstraint);
        }

        return changingOpDesc;
    }

    private static String[] nChooseK(int k, List<String> locations) {
        String[] result = new String[k];
        for (int i = 0; i < k; i++) {
            result[i] = locations.get(i);
        }
        return result;
    }

    private static boolean preProcessingRequired(FeedConnectionId connectionId) {
        MetadataTransactionContext ctx = null;
        Feed feed = null;
        boolean preProcessingRequired = false;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            feed = MetadataManager.INSTANCE.getFeed(ctx, connectionId.getFeedId().getDataverse(), connectionId
                    .getFeedId().getFeedName());
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

    public static Triple<IFeedAdapterFactory, ARecordType, AdapterType> getPrimaryFeedFactoryAndOutput(
            PrimaryFeed feed, FeedPolicyAccessor policyAccessor, MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {

        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        IFeedAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        Triple<IFeedAdapterFactory, ARecordType, AdapterType> feedProps = null;
        AdapterType adapterType = null;
        try {
            adapterName = feed.getAdaptorName();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IFeedAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = ExternalLibraryManager.getLibraryClassLoader(feed.getDataverseName(),
                                libraryName);
                        adapterFactory = (IFeedAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                }
            } else {
                adapterFactoryClassname = AqlMetadataProvider.adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname == null) {
                    adapterFactoryClassname = adapterName;
                }
                adapterFactory = (IFeedAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                adapterType = AdapterType.INTERNAL;
            }

            Map<String, String> configuration = feed.getAdaptorConfiguration();
            configuration.putAll(policyAccessor.getFeedPolicy());
            adapterOutputType = getOutputType(feed, configuration);
            adapterFactory.configure(configuration, adapterOutputType);
            feedProps = new Triple<IFeedAdapterFactory, ARecordType, AdapterType>(adapterFactory, adapterOutputType,
                    adapterType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to create adapter " + e);
        }
        return feedProps;
    }

    private static ARecordType getOutputType(PrimaryFeed feed, Map<String, String> configuration) throws Exception {
        ARecordType outputType = null;
        String fqOutputType = configuration.get(IAdapterFactory.KEY_TYPE_NAME);

        if (fqOutputType == null) {
            throw new IllegalArgumentException("No output type specified");
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
        } else
            throw new IllegalArgumentException("Invalid value for the parameter " + IAdapterFactory.KEY_TYPE_NAME);

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
        } catch (Exception e) {
            if (ctx != null) {
                MetadataManager.INSTANCE.abortTransaction(ctx);
            }
            throw e;
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        return outputType;
    }

    public static String getSecondaryFeedOutput(SecondaryFeed feed, FeedPolicyAccessor policyAccessor,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException, MetadataException {
        String outputType = null;
        String primaryFeedName = feed.getSourceFeedName();
        Feed primaryFeed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, feed.getDataverseName(), primaryFeedName);
        FunctionSignature appliedFunction = primaryFeed.getAppliedFunction();
        if (appliedFunction == null) {
            Triple<IFeedAdapterFactory, ARecordType, AdapterType> result = getPrimaryFeedFactoryAndOutput(
                    (PrimaryFeed) primaryFeed, policyAccessor, mdTxnCtx);
            outputType = result.second.getTypeName();
        } else {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
            if (function != null) {
                if (function.getLanguage().equals(Function.LANGUAGE_AQL)) {
                    throw new NotImplementedException(
                            "Secondary feeds derived from a source feed that has an applied AQL function are not supported yet.");
                } else {
                    outputType = function.getReturnType();
                }
            } else {
                throw new IllegalArgumentException("Function " + appliedFunction
                        + " associated with source feed not found in Metadata.");
            }
        }
        return outputType;
    }

}
