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

package org.apache.asterix.metadata.entities;

import java.util.List;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a named group of compute nodes.
 */
public class NodeGroup implements IMetadataEntity<NodeGroup> {

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
    public NodeGroup addToCache(MetadataCache cache) {
        return cache.addOrUpdateNodeGroup(this);
    }

    @Override
    public NodeGroup dropFromCache(MetadataCache cache) {
        return cache.dropNodeGroup(this);
    }
}
