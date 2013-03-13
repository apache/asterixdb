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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * A factory class for creating an instance of HDFSAdapter
 */
public class HDFSAdapterFactory implements IGenericDatasetAdapterFactory {

    public static final String HDFS_ADAPTER_NAME = "hdfs";

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType atype) throws Exception {
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype);
        hdfsAdapter.configure(configuration);
        return hdfsAdapter;
    }

    @Override
    public String getName() {
        return HDFS_ADAPTER_NAME;
    }

}
