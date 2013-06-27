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

import edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;

/**
 * A factory class for creating the @see {CNNFeedAdapter}.  
 */
public class CNNFeedAdapterFactory implements ITypedDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IDatasourceAdapter createAdapter(Map<String, Object> configuration) throws Exception {
        CNNFeedAdapter cnnFeedAdapter = new CNNFeedAdapter();
        cnnFeedAdapter.configure(configuration);
        return cnnFeedAdapter;
    }

    @Override
    public String getName() {
        return "cnn_feed";
    }

}
