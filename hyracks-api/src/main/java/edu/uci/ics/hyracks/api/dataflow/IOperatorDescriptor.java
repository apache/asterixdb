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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

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
     * Returns the partition constraint requested for scheduling this operator. This value is set as part of Job creation by the client.
     * 
     * @return the partition constraint.
     */
    public PartitionConstraint getPartitionConstraint();

    /**
     * Sets the partition constraint.
     * 
     * @param partitionConstraint
     *            - partition constraint.
     */
    public void setPartitionConstraint(PartitionConstraint partitionConstraint);

    /**
     * Gets the output record descriptor
     * 
     * @return Array of RecordDescriptor, one per output.
     */
    public RecordDescriptor[] getOutputRecordDescriptors();

    /**
     * Contributes the activity graph that describes the behavior of this operator.
     * 
     * @param builder
     *            - graph builder
     */
    public void contributeTaskGraph(IActivityGraphBuilder builder);

    /**
     * Translates this operator descriptor to JSON.
     */
    public JSONObject toJSON() throws JSONException;
}