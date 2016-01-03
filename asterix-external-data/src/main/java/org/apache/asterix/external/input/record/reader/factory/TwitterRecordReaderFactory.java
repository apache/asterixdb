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
package org.apache.asterix.external.input.record.reader.factory;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.TwitterPullRecordReader;
import org.apache.asterix.external.input.record.reader.TwitterPushRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.TwitterUtil;
import org.apache.asterix.external.util.TwitterUtil.AuthenticationConstants;
import org.apache.asterix.external.util.TwitterUtil.SearchAPIConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

import twitter4j.Status;

public class TwitterRecordReaderFactory implements IRecordReaderFactory<Status> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TwitterRecordReaderFactory.class.getName());

    private static final String DEFAULT_INTERVAL = "10"; // 10 seconds
    private static final int INTAKE_CARDINALITY = 1; // degree of parallelism at intake stage

    private Map<String, String> configuration;
    private boolean pull;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(INTAKE_CARDINALITY);
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        TwitterUtil.initializeConfigurationWithAuthInfo(configuration);
        if (!validateConfiguration(configuration)) {
            StringBuilder builder = new StringBuilder();
            builder.append("One or more parameters are missing from adapter configuration\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_KEY + "\n");
            builder.append(AuthenticationConstants.OAUTH_CONSUMER_SECRET + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN + "\n");
            builder.append(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET + "\n");
            throw new Exception(builder.toString());
        }
        if (ExternalDataUtils.isPull(configuration)) {
            pull = true;
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
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(" Parameter " + SearchAPIConstants.INTERVAL + " not defined, using default ("
                            + DEFAULT_INTERVAL + ")");
                }
            }
        } else if (ExternalDataUtils.isPush(configuration)) {
            pull = false;
        } else {
            throw new AsterixException("One of boolean parameters " + ExternalDataConstants.KEY_PULL + " and "
                    + ExternalDataConstants.KEY_PUSH + "must be specified as part of adaptor configuration");
        }
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IRecordReader<? extends Status> createRecordReader(IHyracksTaskContext ctx, int partition) throws Exception {
        IRecordReader<Status> reader;
        if (pull) {
            reader = new TwitterPullRecordReader();
        } else {
            reader = new TwitterPushRecordReader();
        }
        reader.configure(configuration);
        return reader;
    }

    @Override
    public Class<? extends Status> getRecordClass() {
        return Status.class;
    }

    private boolean validateConfiguration(Map<String, String> configuration) {
        String consumerKey = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_KEY);
        String consumerSecret = configuration.get(AuthenticationConstants.OAUTH_CONSUMER_SECRET);
        String accessToken = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN);
        String tokenSecret = configuration.get(AuthenticationConstants.OAUTH_ACCESS_TOKEN_SECRET);
        if (consumerKey == null || consumerSecret == null || accessToken == null || tokenSecret == null) {
            return false;
        }
        return true;
    }
}
