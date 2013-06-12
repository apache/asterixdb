/*
x * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.tools.external.data;

import java.util.Map;

import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class SyntheticTwitterFeedAdapterFactory implements IAdapterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Map<String, Object> configuration;

    @Override
    public String getName() {
        return "synthetic_twitter_feed";
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

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new SyntheticTwitterFeedAdapter(configuration, ctx);
    }

}