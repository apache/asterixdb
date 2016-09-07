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

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

public abstract class AbstractSingleActivityOperatorDescriptor extends AbstractOperatorDescriptor implements IActivity {
    private static final long serialVersionUID = 1L;

    protected final ActivityId activityNodeId;

    public AbstractSingleActivityOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity) {
        super(spec, inputArity, outputArity);
        activityNodeId = new ActivityId(odId, 0);
    }

    @Override
    public ActivityId getActivityId() {
        return activityNodeId;
    }

    @Override
    public final void setOperatorId(OperatorDescriptorId id) {
        super.setOperatorId(id);
        if (activityNodeId != null && !activityNodeId.getOperatorDescriptorId().equals(odId)) {
            activityNodeId.setOperatorDescriptorId(odId);
        }
    }

    @Override
    public final void contributeActivities(IActivityGraphBuilder builder) {
        builder.addActivity(this, this);
        for (int i = 0; i < getInputArity(); ++i) {
            builder.addSourceEdge(i, this, i);
        }
        for (int i = 0; i < getOutputArity(); ++i) {
            builder.addTargetEdge(i, this, i);
        }
    }
}
