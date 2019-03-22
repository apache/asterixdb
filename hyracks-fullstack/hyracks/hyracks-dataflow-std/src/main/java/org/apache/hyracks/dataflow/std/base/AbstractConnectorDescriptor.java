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
package org.apache.hyracks.dataflow.std.base;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractConnectorDescriptor implements IConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    protected ConnectorDescriptorId id;

    protected String displayName;

    public AbstractConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        this.id = spec.createConnectorDescriptor(this);
        displayName = getClass().getName() + "[" + id + "]";
    }

    @Override
    public ConnectorDescriptorId getConnectorId() {
        return id;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode jconn = om.createObjectNode();

        jconn.put("id", String.valueOf(getConnectorId()));
        jconn.put("java-class", getClass().getName());
        jconn.put("display-name", displayName);

        return jconn;
    }

    @Override
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ActivityCluster ac,
            ICCServiceContext serviceCtx) {
        // do nothing
    }

    @Override
    public void setConnectorId(ConnectorDescriptorId id) {
        this.id = id;
    }
}
