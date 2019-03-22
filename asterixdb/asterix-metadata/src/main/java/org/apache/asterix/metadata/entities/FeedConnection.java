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

package org.apache.asterix.metadata.entities;

import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Feed connection records the feed --> dataset mapping.
 */
public class FeedConnection implements IMetadataEntity<FeedConnection> {

    private static final long serialVersionUID = 1L;

    private EntityId feedId;
    private String connectionId;
    private String dataverseName;
    private String feedName;
    private String datasetName;
    private String policyName;
    private String whereClauseBody;
    private String outputType;
    private List<FunctionSignature> appliedFunctions;

    public FeedConnection(String dataverseName, String feedName, String datasetName,
            List<FunctionSignature> appliedFunctions, String policyName, String whereClauseBody, String outputType) {
        this.dataverseName = dataverseName;
        this.feedName = feedName;
        this.datasetName = datasetName;
        this.appliedFunctions = appliedFunctions;
        this.connectionId = feedName + ":" + datasetName;
        this.policyName = policyName;
        this.whereClauseBody = whereClauseBody == null ? "" : whereClauseBody;
        this.outputType = outputType;
        this.feedId = new EntityId(FeedUtils.FEED_EXTENSION_NAME, dataverseName, feedName);
    }

    public List<FunctionSignature> getAppliedFunctions() {
        return appliedFunctions;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeedConnection)) {
            return false;
        }
        return ((FeedConnection) other).getConnectionId().equals(connectionId);
    }

    @Override
    public int hashCode() {
        return connectionId.hashCode();
    }

    @Override
    public FeedConnection addToCache(MetadataCache cache) {
        return null;
    }

    @Override
    public FeedConnection dropFromCache(MetadataCache cache) {
        return null;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public String getFeedName() {
        return feedName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getWhereClauseBody() {
        return whereClauseBody;
    }

    public String getOutputType() {
        return outputType;
    }

    public EntityId getFeedId() {
        return feedId;
    }

    public boolean containsFunction(FunctionSignature signature) {
        return appliedFunctions.contains(signature);
    }
}
