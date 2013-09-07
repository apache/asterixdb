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
package edu.uci.ics.asterix.aql.expression;

import java.util.Map;

import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;

public class ExternalDetailsDecl implements IDatasetDetailsDecl {
    private Map<String, String> properties;
    private String adapter;
    private Identifier nodegroupName;

    public ExternalDetailsDecl(Map<String, String> properties, String adapter, Identifier nodegroupName) {
		this.properties = properties;
		this.adapter = adapter;
		this.nodegroupName = nodegroupName == null ? new Identifier(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)
        : nodegroupName;
	}
    
    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
    
    public void setNodegroupName(Identifier nodegroupName) {
		this.nodegroupName = nodegroupName;
	}
    
    public Identifier getNodegroupName() {
		return nodegroupName;
	}
}
