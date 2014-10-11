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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.functions.ExternalLibraryManager;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.StreamProjectRuntimeFactory;
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
import edu.uci.ics.hyracks.api.job.JobSpecification;

/**
 * A utility class for providing helper functions for feeds
 */
public class FeedUtil {

    private static Logger LOGGER = Logger.getLogger(FeedUtil.class.getName());

    public static boolean isFeedActive(FeedActivity feedActivity) {
        return (feedActivity != null && !(feedActivity.getActivityType().equals(FeedActivityType.FEED_FAILURE) || feedActivity
                .getActivityType().equals(FeedActivityType.FEED_END)));
    }

    private static class LocationConstraint {
        int partition;
        String location;
    }

    public static JobSpecification alterJobSpecificationForFeed(JobSpecification spec,
            FeedConnectionId feedConnectionId, FeedPolicy feedPolicy) {

        FeedPolicyAccessor fpa = new FeedPolicyAccessor(feedPolicy.getProperties());
        boolean alterationRequired = (fpa.collectStatistics() || fpa.continueOnApplicationFailure()
                || fpa.continueOnHardwareFailure() || fpa.isElastic());
        if (!alterationRequired) {
            return spec;
        }

        JobSpecification altered = new JobSpecification(spec.getFrameSize());
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();

        // copy operators
        String operationId = null;
        Map<OperatorDescriptorId, OperatorDescriptorId> oldNewOID = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorMap.entrySet()) {
            operationId = FeedRuntime.FeedRuntimeId.DEFAULT_OPERATION_ID;
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                FeedIntakeOperatorDescriptor orig = (FeedIntakeOperatorDescriptor) opDesc;
                FeedIntakeOperatorDescriptor fiop;
                if (orig.getAdapterFactory() != null) {
                    fiop = new FeedIntakeOperatorDescriptor(altered, orig.getFeedId(), orig.getAdapterFactory(),
                            (ARecordType) orig.getOutputType(), orig.getRecordDescriptor(), orig.getFeedPolicy());
                } else {
                    fiop = new FeedIntakeOperatorDescriptor(altered, orig.getFeedId(), orig.getAdapterLibraryName(),
                            orig.getAdapterFactoryClassName(), orig.getAdapterConfiguration(),
                            (ARecordType) orig.getOutputType(), orig.getRecordDescriptor(), orig.getFeedPolicy());
                }
                oldNewOID.put(opDesc.getOperatorId(), fiop.getOperatorId());
            } else if (opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor) {
                operationId = ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                FeedMetaOperatorDescriptor metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc,
                        feedPolicy, FeedRuntimeType.STORAGE, operationId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            } else if (opDesc instanceof AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) {
                operationId = ((AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                FeedMetaOperatorDescriptor metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc,
                        feedPolicy, FeedRuntimeType.STORAGE, operationId);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            } else {
                FeedRuntimeType runtimeType = null;
                if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                    IPushRuntimeFactory runtimeFactory = ((AlgebricksMetaOperatorDescriptor) opDesc).getPipeline()
                            .getRuntimeFactories()[0];
                    if (runtimeFactory instanceof AssignRuntimeFactory) {
                        runtimeType = FeedRuntimeType.COMPUTE;
                    } else if (runtimeFactory instanceof StreamProjectRuntimeFactory) {
                        runtimeType = FeedRuntimeType.COMMIT;
                    }
                }
                FeedMetaOperatorDescriptor metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc,
                        feedPolicy, runtimeType, operationId);

                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
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

    public static Triple<IAdapterFactory, ARecordType, AdapterType> getFeedFactoryAndOutput(Feed feed,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException {

        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        IAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        Triple<IAdapterFactory, ARecordType, AdapterType> feedProps = null;
        try {
            adapterName = feed.getAdapterName();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }

            if (adapterEntity != null) {
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterEntity.getType()) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = ExternalLibraryManager.getLibraryClassLoader(feed.getDataverseName(),
                                libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                }
            } else {
                adapterFactoryClassname = AqlMetadataProvider.adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname == null) {
                    adapterFactoryClassname = adapterName;
                }
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            }

            Map<String, String> configuration = feed.getAdapterConfiguration();

            switch (adapterFactory.getAdapterType()) {
                case TYPED:
                    ((ITypedAdapterFactory) adapterFactory).configure(configuration);
                    adapterOutputType = ((ITypedAdapterFactory) adapterFactory).getAdapterOutputType();
                    break;
                case GENERIC:
                    String outputTypeName = configuration.get(IGenericAdapterFactory.KEY_TYPE_NAME);
                    if (outputTypeName == null) {
                        throw new IllegalArgumentException(
                                "You must specify the datatype associated with the incoming data. Datatype is specified by the "
                                        + IGenericAdapterFactory.KEY_TYPE_NAME + " configuration parameter");
                    }
                    Datatype datatype = MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                            feed.getDataverseName(), outputTypeName);
                    if (datatype == null) {
                        throw new Exception("no datatype \"" + outputTypeName + "\" in dataverse \"" + feed.getDataverseName() + "\"");
                    }
                    adapterOutputType = (ARecordType) datatype.getDatatype();
                    ((IGenericAdapterFactory) adapterFactory).configure(configuration, (ARecordType) adapterOutputType, false, null);
                    break;
                default:
                    throw new IllegalStateException(" Unknown factory type for " + adapterFactoryClassname);
            }

            feedProps = new Triple<IAdapterFactory, ARecordType, AdapterType>(adapterFactory, adapterOutputType,
                    adapterEntity.getType());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException("unable to create adapter  " + e);
        }
        return feedProps;
    }
}
