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

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class AbstractOperatorDescriptor implements IOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    protected OperatorDescriptorId odId;

    protected String[] partitions;

    protected final RecordDescriptor[] recordDescriptors;

    protected final int inputArity;

    protected final int outputArity;

    protected String displayName;

    public AbstractOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity) {
        odId = spec.createOperatorDescriptorId(this);
        this.inputArity = inputArity;
        this.outputArity = outputArity;
        recordDescriptors = new RecordDescriptor[outputArity];
        displayName = getClass().getName() + "[" + odId + "]";
    }

    @Override
    public final OperatorDescriptorId getOperatorId() {
        return odId;
    }

    @Override
    public void setOperatorId(OperatorDescriptorId id) {
        this.odId = id;
    }

    @Override
    public int getInputArity() {
        return inputArity;
    }

    @Override
    public int getOutputArity() {
        return outputArity;
    }

    @Override
    public RecordDescriptor[] getOutputRecordDescriptors() {
        return recordDescriptors;
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
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ICCApplicationContext appCtx) {
        // do nothing
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject jop = new JSONObject();
        jop.put("id", String.valueOf(getOperatorId()));
        jop.put("java-class", getClass().getName());
        jop.put("in-arity", getInputArity());
        jop.put("out-arity", getOutputArity());
        jop.put("display-name", displayName);
        return jop;
    }
}
