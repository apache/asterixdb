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

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;

public abstract class AbstractConnectorDescriptor implements IConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    protected final ConnectorDescriptorId id;

    protected String displayName;

    public AbstractConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        this.id = spec.createConnectorDescriptor(this);
        displayName = getClass().getName() + "[" + id + "]";
    }

    public ConnectorDescriptorId getConnectorId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject jconn = new JSONObject();

        jconn.put("id", String.valueOf(getConnectorId()));
        jconn.put("java-class", getClass().getName());
        jconn.put("display-name", displayName);

        return jconn;
    }

    @Override
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ActivityCluster ac,
            ICCApplicationContext appCtx) {
        // do nothing
    }
}