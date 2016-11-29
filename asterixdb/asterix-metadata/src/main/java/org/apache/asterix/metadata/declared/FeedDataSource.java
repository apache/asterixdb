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
package org.apache.asterix.metadata.declared;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedDataSource extends DataSource implements IMutationDataSource {

    private final Feed feed;
    private final EntityId sourceFeedId;
    private final IFeed.FeedType sourceFeedType;
    private final FeedRuntimeType location;
    private final String targetDataset;
    private final String[] locations;
    private final int computeCardinality;
    private final List<IAType> pkTypes;
    private final List<ScalarFunctionCallExpression> keyAccessExpression;

    public FeedDataSource(Feed feed, DataSourceId id, String targetDataset, IAType itemType, IAType metaType,
            List<IAType> pkTypes, List<List<String>> partitioningKeys,
            List<ScalarFunctionCallExpression> keyAccessExpression, EntityId sourceFeedId,
            IFeed.FeedType sourceFeedType, FeedRuntimeType location, String[] locations, INodeDomain domain)
            throws AlgebricksException {
        super(id, itemType, metaType, Type.FEED, domain);
        this.feed = feed;
        this.targetDataset = targetDataset;
        this.sourceFeedId = sourceFeedId;
        this.sourceFeedType = sourceFeedType;
        this.location = location;
        this.locations = locations;
        this.pkTypes = pkTypes;
        this.keyAccessExpression = keyAccessExpression;
        this.computeCardinality = ClusterStateManager.INSTANCE.getParticipantNodes().size();
        initFeedDataSource();
    }

    public Feed getFeed() {
        return feed;
    }

    @Override
    public IAType[] getSchemaTypes() {
        return schemaTypes;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public EntityId getSourceFeedId() {
        return sourceFeedId;
    }

    public FeedRuntimeType getLocation() {
        return location;
    }

    public String[] getLocations() {
        return locations;
    }

    private void initFeedDataSource() {
        int i = 0;
        // record + meta (if exists) + PKs (if exists)
        schemaTypes = new IAType[(1 + (metaItemType != null ? 1 : 0) + (pkTypes != null ? pkTypes.size() : 0))];
        schemaTypes[i++] = itemType;
        if (metaItemType != null) {
            schemaTypes[i++] = metaItemType;
        }
        if (pkTypes != null) {
            for (IAType type : pkTypes) {
                schemaTypes[i++] = type;
            }
        }
    }

    public IFeed.FeedType getSourceFeedType() {
        return sourceFeedType;
    }

    public int getComputeCardinality() {
        return computeCardinality;
    }

    public List<IAType> getPkTypes() {
        return pkTypes;
    }

    @Override
    public List<ScalarFunctionCallExpression> getKeyAccessExpression() {
        return keyAccessExpression;
    }

    @Override
    public LogicalVariable getMetaVariable(List<LogicalVariable> dataScanVariables) {
        return metaItemType == null ? null : dataScanVariables.get(1);
    }

    @Override
    public LogicalVariable getDataRecordVariable(List<LogicalVariable> dataScanVariables) {
        return dataScanVariables.get(0);
    }

    @Override
    public boolean isChange() {
        return pkTypes != null;
    }

    @Override
    public List<LogicalVariable> getPkVars(List<LogicalVariable> allVars) {
        if (pkTypes == null) {
            return null;
        }
        if (metaItemType != null) {
            return allVars.subList(2, allVars.size());
        } else {
            return allVars.subList(1, allVars.size());
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        try {
            ARecordType feedOutputType = (ARecordType) itemType;
            ISerializerDeserializer payloadSerde = NonTaggedDataFormat.INSTANCE.getSerdeProvider()
                    .getSerializerDeserializer(feedOutputType);
            ArrayList<ISerializerDeserializer> serdes = new ArrayList<>();
            serdes.add(payloadSerde);
            if (metaItemType != null) {
                serdes.add(SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaItemType));
            }
            if (pkTypes != null) {
                for (IAType type : pkTypes) {
                    serdes.add(SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(type));
                }
            }
            RecordDescriptor feedDesc = new RecordDescriptor(
                    serdes.toArray(new ISerializerDeserializer[serdes.size()]));
            FeedPolicyEntity feedPolicy = (FeedPolicyEntity) getProperties()
                    .get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY);
            if (feedPolicy == null) {
                throw new AlgebricksException("Feed not configured with a policy");
            }
            feedPolicy.getProperties().put(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY, feedPolicy.getPolicyName());
            FeedConnectionId feedConnectionId = new FeedConnectionId(getId().getDataverseName(),
                    getId().getDatasourceName(), getTargetDataset());
            FeedCollectOperatorDescriptor feedCollector = new FeedCollectOperatorDescriptor(jobSpec, feedConnectionId,
                    getSourceFeedId(), feedOutputType, feedDesc, feedPolicy.getProperties(), getLocation());

            return new Pair<>(feedCollector, new AlgebricksAbsolutePartitionConstraint(getLocations()));

        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return true;
    }
}