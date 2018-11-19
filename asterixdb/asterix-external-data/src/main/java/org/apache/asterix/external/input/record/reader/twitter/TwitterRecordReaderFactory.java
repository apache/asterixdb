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
package org.apache.asterix.external.input.record.reader.twitter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.AuthenticationConstants;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.FilterQuery;

public class TwitterRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private static final String DEFAULT_INTERVAL = "10"; // 10 seconds
    private static final int INTAKE_CARDINALITY = 1; // degree of parallelism at intake stage

    private Map<String, String> configuration;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    private transient IServiceContext serviceCtx;

    private static final List<String> recordReaderNames = Collections.unmodifiableList(Arrays.asList(
            ExternalDataConstants.KEY_ADAPTER_NAME_TWITTER_PULL, ExternalDataConstants.KEY_ADAPTER_NAME_TWITTER_PUSH,
            ExternalDataConstants.KEY_ADAPTER_NAME_PUSH_TWITTER, ExternalDataConstants.KEY_ADAPTER_NAME_PULL_TWITTER,
            ExternalDataConstants.KEY_ADAPTER_NAME_TWITTER_USER_STREAM));

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(
                (ICcApplicationContext) serviceCtx.getApplicationContext(), clusterLocations, INTAKE_CARDINALITY);
        return clusterLocations;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws AsterixException {
        try {
            Class.forName("twitter4j.Twitter");
        } catch (ClassNotFoundException e) {
            throw new AsterixException(ErrorCode.ADAPTER_TWITTER_TWITTER4J_LIB_NOT_FOUND, e);
        }

        this.configuration = configuration;
        this.serviceCtx = serviceCtx;
        TwitterUtil.initializeConfigurationWithAuthInfo(configuration);
        if (!validateConfiguration(configuration)) {
            StringBuilder builder = new StringBuilder();
            builder.append("One or more parameters are missing from adapter configuration\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);
            throw new AsterixException(builder.toString());
        }

        if (configuration.get(ExternalDataConstants.KEY_READER)
                .equals(ExternalDataConstants.KEY_ADAPTER_NAME_PULL_TWITTER)) {
            if (configuration.get(SearchAPIConstants.QUERY) == null) {
                throw new AsterixException(
                        "parameter " + SearchAPIConstants.QUERY + " not specified as part of adaptor configuration");
            }
            String interval = configuration.get(SearchAPIConstants.INTERVAL);
            if (interval != null) {
                try {
                    Integer.parseInt(interval);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                            "parameter " + SearchAPIConstants.INTERVAL + " is defined incorrectly, expecting a number");
                }
            } else {
                configuration.put(SearchAPIConstants.INTERVAL, DEFAULT_INTERVAL);
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(" Parameter " + SearchAPIConstants.INTERVAL + " not defined, using default ("
                            + DEFAULT_INTERVAL + ")");
                }
            }
        }
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        IRecordReader<? extends char[]> recordReader;
        switch (configuration.get(ExternalDataConstants.KEY_READER)) {
            case ExternalDataConstants.KEY_ADAPTER_NAME_PULL_TWITTER:
                recordReader = new TwitterPullRecordReader(TwitterUtil.getTwitterService(configuration),
                        configuration.get(SearchAPIConstants.QUERY),
                        Integer.parseInt(configuration.get(SearchAPIConstants.INTERVAL)));
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_PUSH_TWITTER:
                FilterQuery query;
                try {
                    query = TwitterUtil.getFilterQuery(configuration);
                    recordReader = (query == null)
                            ? new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                                    TwitterUtil.getTweetListener())
                            : new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                                    TwitterUtil.getTweetListener(), query);
                } catch (AsterixException e) {
                    throw HyracksDataException.create(e);
                }
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_TWITTER_USER_STREAM:
                recordReader = new TwitterPushRecordReader(TwitterUtil.getTwitterStream(configuration),
                        TwitterUtil.getUserTweetsListener());
                break;
            default:
                throw new HyracksDataException("No Record reader found!");
        }
        return recordReader;
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

    private boolean validateConfiguration(Map<String, String> configuration) {
        String consumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String consumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String accessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String tokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);
        if ((consumerKey == null) || (consumerSecret == null) || (accessToken == null) || (tokenSecret == null)) {
            return false;
        }
        return true;
    }

}
