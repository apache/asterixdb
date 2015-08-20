/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.api.constraints;

import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PartitionConstraintHelper {
    public static void addPartitionCountConstraint(JobSpecification spec, IOperatorDescriptor op, int count) {
        spec.addUserConstraint(new Constraint(new PartitionCountExpression(op.getOperatorId()), new ConstantExpression(
                count)));
    }

    public static void addLocationChoiceConstraint(JobSpecification spec, IOperatorDescriptor op, String[][] choices) {
        addPartitionCountConstraint(spec, op, choices.length);
        for (int i = 0; i < choices.length; ++i) {
            spec.addUserConstraint(new Constraint(new PartitionLocationExpression(op.getOperatorId(), i),
                    new ConstantExpression(choices[i])));
        }
    }

    public static void addAbsoluteLocationConstraint(JobSpecification spec, IOperatorDescriptor op, String... locations) {
        addPartitionCountConstraint(spec, op, locations.length);
        for (int i = 0; i < locations.length; ++i) {
            spec.addUserConstraint(new Constraint(new PartitionLocationExpression(op.getOperatorId(), i),
                    new ConstantExpression(locations[i])));
        }
    }
}