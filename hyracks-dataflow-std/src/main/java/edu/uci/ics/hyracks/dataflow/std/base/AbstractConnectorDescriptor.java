/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.base;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public abstract class AbstractConnectorDescriptor implements IConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    protected final ConnectorDescriptorId id;

    protected String displayName;

    public AbstractConnectorDescriptor(JobSpecification spec) {
        this.id = spec.createConnectorDescriptor();
        spec.getConnectorMap().put(id, this);
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
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, JobActivityGraph plan,
            ICCApplicationContext appCtx) {
        // do nothing
    }
}