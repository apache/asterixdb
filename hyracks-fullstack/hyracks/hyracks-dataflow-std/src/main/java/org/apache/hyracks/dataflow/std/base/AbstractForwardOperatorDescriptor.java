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

import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

// TODO(ali): forward operator should probably be moved to asterix layer
public abstract class AbstractForwardOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    protected static final int FORWARD_DATA_ACTIVITY_ID = 0;
    protected static final int SIDE_DATA_ACTIVITY_ID = 1;
    protected String sideDataKey;

    /**
     * @param spec used to create the operator id.
     * @param sideDataKey the key used to store the output of the side activity
     * @param outputRecordDescriptor the output schema of this operator.
     */
    public AbstractForwardOperatorDescriptor(IOperatorDescriptorRegistry spec, String sideDataKey,
            RecordDescriptor outputRecordDescriptor) {
        super(spec, 2, 1);
        outRecDescs[0] = outputRecordDescriptor;
        this.sideDataKey = sideDataKey;
    }

    /**
     * @return the forward data activity
     */
    public abstract AbstractActivityNode createForwardDataActivity();

    /**
     * @return the side data activity
     */
    public abstract AbstractActivityNode createSideDataActivity();

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        AbstractActivityNode forwardDataActivity = createForwardDataActivity();
        AbstractActivityNode sideDataActivity = createSideDataActivity();

        // side data activity, its input is coming through the operator's in-port = 1 & activity's in-port = 0
        builder.addActivity(this, sideDataActivity);
        builder.addSourceEdge(1, sideDataActivity, 0);

        // forward data activity, its input is coming through the operator's in-port = 0 & activity's in-port = 0
        builder.addActivity(this, forwardDataActivity);
        builder.addSourceEdge(0, forwardDataActivity, 0);

        // forward data activity will wait for the side data activity
        builder.addBlockingEdge(sideDataActivity, forwardDataActivity);

        // data leaves from the operator's out-port = 0 & forward data activity's out-port = 0
        builder.addTargetEdge(0, forwardDataActivity, 0);
    }
}
