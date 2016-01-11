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

import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.Feed.FeedType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class FeedDataSource extends AqlDataSource {

    private Feed feed;
    private final FeedId sourceFeedId;
    private final FeedType sourceFeedType;
    private final ConnectionLocation location;
    private final String targetDataset;
    private final String[] locations;
    private final int computeCardinality;

    public FeedDataSource(AqlSourceId id, String targetDataset, IAType itemType, AqlDataSourceType dataSourceType,
            FeedId sourceFeedId, FeedType sourceFeedType, ConnectionLocation location, String[] locations)
                    throws AlgebricksException {
        super(id, itemType, dataSourceType);
        this.targetDataset = targetDataset;
        this.sourceFeedId = sourceFeedId;
        this.sourceFeedType = sourceFeedType;
        this.location = location;
        this.locations = locations;
        this.computeCardinality = AsterixClusterProperties.INSTANCE.getParticipantNodes().size();
        MetadataTransactionContext ctx = null;
        try {
            MetadataManager.INSTANCE.acquireReadLatch();
            ctx = MetadataManager.INSTANCE.beginTransaction();
            this.feed = MetadataManager.INSTANCE.getFeed(ctx, id.getDataverseName(), id.getDatasourceName());
            MetadataManager.INSTANCE.commitTransaction(ctx);
            initFeedDataSource(itemType);
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    throw new IllegalStateException("Unable to abort " + e2.getMessage());
                }
            }

        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
    }

    public Feed getFeed() {
        return feed;
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

    private void initFeedDataSource(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        INodeDomain domainForExternalData = new INodeDomain() {
            @Override
            public Integer cardinality() {
                return null;
            }

            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }
        };
        domain = domainForExternalData;
    }

    public FeedType getSourceFeedType() {
        return sourceFeedType;
    }

    public int getComputeCardinality() {
        return computeCardinality;
    }
}
