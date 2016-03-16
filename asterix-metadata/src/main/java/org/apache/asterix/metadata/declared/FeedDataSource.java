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

import java.util.List;

import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.api.IFeedLifecycleListener.ConnectionLocation;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class FeedDataSource extends AqlDataSource {

    private final Feed feed;
    private final FeedId sourceFeedId;
    private final IFeed.FeedType sourceFeedType;
    private final ConnectionLocation location;
    private final String targetDataset;
    private final String[] locations;
    private final int computeCardinality;
    private final List<IAType> pkTypes;
    private final List<ScalarFunctionCallExpression> keyAccessExpression;

    public FeedDataSource(Feed feed, AqlSourceId id, String targetDataset, IAType itemType, IAType metaType,
            List<IAType> pkTypes, List<List<String>> partitioningKeys,
            List<ScalarFunctionCallExpression> keyAccessExpression, FeedId sourceFeedId, IFeed.FeedType sourceFeedType,
            ConnectionLocation location, String[] locations, INodeDomain domain) throws AlgebricksException {
        super(id, itemType, metaType, AqlDataSourceType.FEED, domain);
        this.feed = feed;
        this.targetDataset = targetDataset;
        this.sourceFeedId = sourceFeedId;
        this.sourceFeedType = sourceFeedType;
        this.location = location;
        this.locations = locations;
        this.pkTypes = pkTypes;
        this.keyAccessExpression = keyAccessExpression;
        this.computeCardinality = AsterixClusterProperties.INSTANCE.getParticipantNodes().size();
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

    public FeedId getSourceFeedId() {
        return sourceFeedId;
    }

    public ConnectionLocation getLocation() {
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

    public boolean isChange() {
        return pkTypes != null;
    }

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
}