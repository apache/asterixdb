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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating an instance of PullBasedTwitterAdapter.
 * This adapter provides the functionality of fetching tweets from Twitter service
 * via pull-based Twitter API.
 */
public class PullBasedTwitterAdapterFactory implements IAdapterFactory {
    private static final long serialVersionUID = 1L;
    public static final String PULL_BASED_TWITTER_ADAPTER_NAME = "pull_twitter";

    private Map<String, Object> configuration;

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new PullBasedTwitterAdapter(configuration, ctx);
    }

    @Override
    public String getName() {
        return PULL_BASED_TWITTER_ADAPTER_NAME;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

}
