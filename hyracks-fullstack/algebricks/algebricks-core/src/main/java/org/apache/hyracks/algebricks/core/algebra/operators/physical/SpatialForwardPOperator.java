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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractForwardOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SpatialForwardOperatorDescriptor;

public class SpatialForwardPOperator extends AbstractForwardPOperator {
    @Override
    public AbstractForwardOperatorDescriptor getOperatorDescriptor(IHyracksJobBuilder builder,
            ForwardOperator forwardOp, RecordDescriptor dataInputDescriptor) {
        return new SpatialForwardOperatorDescriptor(builder.getJobSpec(), forwardOp.getSideDataKey(),
                dataInputDescriptor);
    }
}
