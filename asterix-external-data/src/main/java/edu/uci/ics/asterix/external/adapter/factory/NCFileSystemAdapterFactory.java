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

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * Factory class for creating an instance of NCFileSystemAdapter. An
 * NCFileSystemAdapter reads external data residing on the local file system of
 * an NC.
 */
public class NCFileSystemAdapterFactory implements IGenericDatasetAdapterFactory {
    private static final long serialVersionUID = 1L;
    public static final String NC_FILE_SYSTEM_ADAPTER_NAME = "localfs";

    @Override
    public IDatasourceAdapter createAdapter(Map<String, Object> configuration, IAType atype) throws Exception {
        NCFileSystemAdapter fsAdapter = new NCFileSystemAdapter(atype);
        fsAdapter.configure(configuration);
        return fsAdapter;
    }

    @Override
    public String getName() {
        return NC_FILE_SYSTEM_ADAPTER_NAME;
    }
}
