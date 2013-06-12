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

package edu.uci.ics.asterix.metadata.entities;

import java.util.List;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a named group of compute nodes.
 */
public class NodeGroup implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    // Enforced to be unique within an Asterix cluster.
    private final String groupName;
    private final List<String> nodeNames;

    public NodeGroup(String groupName, List<String> nodeNames) {
        this.groupName = groupName;
        this.nodeNames = nodeNames;
    }

    public String getNodeGroupName() {
        return this.groupName;
    }

    public List<String> getNodeNames() {
        return this.nodeNames;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addNodeGroupIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropNodeGroup(this);
    }
}
