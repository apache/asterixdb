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
package org.apache.asterix.external.input.stream.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.input.stream.TwitterFirehoseInputStream;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Factory class for creating @see{TwitterFirehoseFeedAdapter}. The adapter
 * simulates a twitter firehose with tweets being "pushed" into Asterix at a
 * configurable rate measured in terms of TPS (tweets/second). The stream of
 * tweets lasts for a configurable duration (measured in seconds).
 */
public class TwitterFirehoseStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;

    /**
     * Degree of parallelism for feed ingestion activity. Defaults to 1. This
     * determines the count constraint for the ingestion operator.
     **/
    private static final String KEY_INGESTION_CARDINALITY = "ingestion-cardinality";

    /**
     * The absolute locations where ingestion operator instances will be placed.
     **/
    private static final String KEY_INGESTION_LOCATIONS = "ingestion-location";

    private Map<String, String> configuration;
    private transient IServiceContext serviceCtx;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        String ingestionCardinalityParam = configuration.get(KEY_INGESTION_CARDINALITY);
        String ingestionLocationParam = configuration.get(KEY_INGESTION_LOCATIONS);
        String[] locations = null;
        if (ingestionLocationParam != null) {
            locations = ingestionLocationParam.split(",");
        }
        int count = locations != null ? locations.length : 1;
        if (ingestionCardinalityParam != null) {
            count = Integer.parseInt(ingestionCardinalityParam);
        }
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        List<String> chosenLocations = new ArrayList<>();
        String[] availableLocations = locations != null ? locations
                : appCtx.getClusterStateManager().getParticipantNodes().toArray(new String[] {});
        for (int i = 0, k = 0; i < count; i++, k = (k + 1) % availableLocations.length) {
            chosenLocations.add(availableLocations[k]);
        }
        return new AlgebricksAbsolutePartitionConstraint(chosenLocations.toArray(new String[] {}));
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) {
        this.serviceCtx = serviceCtx;
        this.configuration = configuration;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        try {
            return new TwitterFirehoseInputStream(configuration, partition);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
