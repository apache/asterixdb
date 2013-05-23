/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import edu.uci.ics.asterix.external.adapter.factory.ITypedDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;

/**
 * Factory class for creating @see{RateControllerFileSystemBasedAdapter} The
 * adapter simulates a feed from the contents of a source file. The file can be
 * on the local file system or on HDFS. The feed ends when the content of the
 * source file has been ingested.
 */
public class SyntheticTwitterFeedAdapterFactory implements ITypedDatasetAdapterFactory {

    @Override
    public String getName() {
        return "synthetic_twitter_feed";
    }

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception {
        return new SyntheticTwitterFeedAdapter(configuration);
    }

}