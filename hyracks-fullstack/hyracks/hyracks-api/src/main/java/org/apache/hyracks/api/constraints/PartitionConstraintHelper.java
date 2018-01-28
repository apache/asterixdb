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
package org.apache.hyracks.api.constraints;

import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class PartitionConstraintHelper {
    public static void addPartitionCountConstraint(JobSpecification spec, IOperatorDescriptor op, int count) {
        spec.addUserConstraint(
                new Constraint(new PartitionCountExpression(op.getOperatorId()), new ConstantExpression(count)));
    }

    public static void addAbsoluteLocationConstraint(JobSpecification spec, IOperatorDescriptor op,
            String... locations) {
        addPartitionCountConstraint(spec, op, locations.length);
        for (int i = 0; i < locations.length; ++i) {
            spec.addUserConstraint(new Constraint(new PartitionLocationExpression(op.getOperatorId(), i),
                    new ConstantExpression(locations[i])));
        }
    }
}
