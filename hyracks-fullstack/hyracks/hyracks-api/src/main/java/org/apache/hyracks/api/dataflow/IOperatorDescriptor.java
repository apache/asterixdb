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
package org.apache.hyracks.api.dataflow;

import java.io.Serializable;

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Descriptor for operators in Hyracks.
 *
 * @author vinayakb
 */
public interface IOperatorDescriptor extends Serializable {
    /**
     * Returns the id of the operator.
     *
     * @return operator id
     */
    public OperatorDescriptorId getOperatorId();

    /**
     * Sets the id of the operator.
     *
     * @param id
     */
    void setOperatorId(OperatorDescriptorId id);

    /**
     * Returns the number of inputs into this operator.
     *
     * @return Number of inputs.
     */
    public int getInputArity();

    /**
     * Returns the number of outputs out of this operator.
     *
     * @return Number of outputs.
     */
    public int getOutputArity();

    /**
     * Gets the output record descriptor
     *
     * @return Array of RecordDescriptor, one per output.
     */
    public RecordDescriptor[] getOutputRecordDescriptors();

    /**
     * Contributes the activity graph that describes the behavior of this
     * operator.
     *
     * @param builder
     *            - graph builder
     */
    public void contributeActivities(IActivityGraphBuilder builder);

    /**
     * Contributes any scheduling constraints imposed by this operator.
     *
     * @param constraintAcceptor
     *            - Constraint Acceptor
     * @param plan
     *            - Job Plan
     */
    public void contributeSchedulingConstraints(IConstraintAcceptor constraintAcceptor, ICCApplicationContext appCtx);

    /**
     * Gets the display name.
     */
    public String getDisplayName();

    /**
     * Sets the display name.
     */
    public void setDisplayName(String displayName);

    /**
     * Translates this operator descriptor to JSON.
     */
    public JSONObject toJSON() throws JSONException;
}
