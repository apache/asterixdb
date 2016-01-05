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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{TwitterFirehoseFeedAdapter}. The adapter
 * simulates a twitter firehose with tweets being "pushed" into Asterix at a
 * configurable rate measured in terms of TPS (tweets/second). The stream of
 * tweets lasts for a configurable duration (measured in seconds).
 */
public class TwitterFirehoseStreamProviderFactory implements IInputStreamProviderFactory {

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

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
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

        List<String> chosenLocations = new ArrayList<String>();
        String[] availableLocations = locations != null ? locations
                : AsterixClusterProperties.INSTANCE.getParticipantNodes().toArray(new String[] {});
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
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IInputStreamProvider createInputStreamProvider(IHyracksTaskContext ctx, int partition) throws Exception {
        return null;
    }
}