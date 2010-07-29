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

import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public abstract class AbstractSingleActivityOperatorDescriptor extends AbstractOperatorDescriptor implements
        IActivityNode {
    private static final long serialVersionUID = 1L;

    protected final ActivityNodeId activityNodeId;

    public AbstractSingleActivityOperatorDescriptor(JobSpecification spec, int inputArity, int outputArity) {
        super(spec, inputArity, outputArity);
        activityNodeId = new ActivityNodeId(odId, UUID.randomUUID());
    }

    @Override
    public ActivityNodeId getActivityNodeId() {
        return activityNodeId;
    }

    @Override
    public final IOperatorDescriptor getOwner() {
        return this;
    }

    @Override
    public final void contributeTaskGraph(IActivityGraphBuilder builder) {
        builder.addTask(this);
        for (int i = 0; i < getInputArity(); ++i) {
            builder.addSourceEdge(i, this, i);
        }
        for (int i = 0; i < getOutputArity(); ++i) {
            builder.addTargetEdge(i, this, i);
        }
    }
}