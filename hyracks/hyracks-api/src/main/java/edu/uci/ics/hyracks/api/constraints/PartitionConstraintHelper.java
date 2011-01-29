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
package edu.uci.ics.hyracks.api.constraints;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.constraints.expressions.BelongsToExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.EnumeratedCollectionExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.RelationalExpression;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PartitionConstraintHelper {
    public static void addPartitionCountConstraint(JobSpecification spec, IOperatorDescriptor op, int count) {
        spec.addUserConstraint(new RelationalExpression(new PartitionCountExpression(op.getOperatorId()),
                new ConstantExpression(count), RelationalExpression.Operator.EQUAL));
    }

    public static void addLocationChoiceConstraint(JobSpecification spec, IOperatorDescriptor op, String[][] choices) {
        addPartitionCountConstraint(spec, op, choices.length);
        for (int i = 0; i < choices.length; ++i) {
            String[] choice = choices[i];
            List<ConstraintExpression> choiceExprs = new ArrayList<ConstraintExpression>();
            for (String c : choice) {
                choiceExprs.add(new ConstantExpression(c));
            }
            spec.addUserConstraint(new BelongsToExpression(new PartitionLocationExpression(op.getOperatorId(), i),
                    new EnumeratedCollectionExpression(choiceExprs)));
        }
    }

    public static void addAbsoluteLocationConstraint(JobSpecification spec, IOperatorDescriptor op, String... locations) {
        addPartitionCountConstraint(spec, op, locations.length);
        for (int i = 0; i < locations.length; ++i) {
            List<ConstraintExpression> choiceExprs = new ArrayList<ConstraintExpression>();
            choiceExprs.add(new ConstantExpression(locations[i]));
            spec.addUserConstraint(new BelongsToExpression(new PartitionLocationExpression(op.getOperatorId(), i),
                    new EnumeratedCollectionExpression(choiceExprs)));
        }
    }
}