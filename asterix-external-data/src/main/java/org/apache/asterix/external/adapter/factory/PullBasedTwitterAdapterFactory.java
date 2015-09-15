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
package org.apache.asterix.external.adapter.factory;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.external.dataset.adapter.PullBasedTwitterAdapter;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating an instance of PullBasedTwitterAdapter.
 * This adapter provides the functionality of fetching tweets from Twitter service
 * via pull-based Twitter API.
 */
public class PullBasedTwitterAdapterFactory implements IFeedAdapterFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PullBasedTwitterAdapterFactory.class.getName());

    public static final String PULL_BASED_TWITTER_ADAPTER_NAME = "pull_twitter";

    private static final String DEFAULT_INTERVAL = "10"; // 10 seconds
    private static final int INTAKE_CARDINALITY = 1; // degree of parallelism at intake stage 

    private ARecordType outputType;

    private Map<String, String> configuration;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        return new PullBasedTwitterAdapter(configuration, outputType, ctx);
    }

    @Override
    public String getName() {
        return PULL_BASED_TWITTER_ADAPTER_NAME;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.outputType = outputType;
        this.configuration = configuration;
        TwitterUtil.initializeConfigurationWithAuthInfo(configuration);

        if (configuration.get(SearchAPIConstants.QUERY) == null) {
            throw new AsterixException("parameter " + SearchAPIConstants.QUERY
                    + " not specified as part of adaptor configuration");
        }

        String interval = configuration.get(SearchAPIConstants.INTERVAL);
        if (interval != null) {
            try {
                Integer.parseInt(interval);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("parameter " + SearchAPIConstants.INTERVAL
                        + " is defined incorrectly, expecting a number");
            }
        } else {
            configuration.put(SearchAPIConstants.INTERVAL, DEFAULT_INTERVAL);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning(" Parameter " + SearchAPIConstants.INTERVAL + " not defined, using default ("
                        + DEFAULT_INTERVAL + ")");
            }
        }

    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(INTAKE_CARDINALITY);
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

}
