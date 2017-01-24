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
package org.apache.hyracks.control.cc.executor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;

public class PartitionConstraintSolver {
    private final Map<LValueConstraintExpression, Set<ConstraintExpression>> constraints;

    public PartitionConstraintSolver() {
        constraints = new HashMap<LValueConstraintExpression, Set<ConstraintExpression>>();
    }

    public void addConstraints(Collection<Constraint> constraintCollection) {
        for (Constraint c : constraintCollection) {
            addConstraint(c);
        }
    }

    public void addConstraint(Constraint c) {
        Set<ConstraintExpression> rValues = constraints.get(c.getLValue());
        if (rValues == null) {
            rValues = new HashSet<>();
            constraints.put(c.getLValue(), rValues);
        }
        rValues.add(c.getRValue());
    }

    public void solve(Collection<LValueConstraintExpression> targetSet) {
        Set<LValueConstraintExpression> inProcess = new HashSet<>();
        for (LValueConstraintExpression lv : targetSet) {
            solveLValue(lv, inProcess);
        }
    }

    private Solution solve(ConstraintExpression ce, Set<LValueConstraintExpression> inProcess) {
        switch (ce.getTag()) {
            case CONSTANT:
                return new Solution(((ConstantExpression) ce).getValue(), Solution.Status.FOUND);
            case PARTITION_COUNT:
            case PARTITION_LOCATION:
                return solveLValue((LValueConstraintExpression) ce, inProcess);
            default:
                return null;
        }
    }

    private Solution solveLValue(LValueConstraintExpression lv, Set<LValueConstraintExpression> inProcess) {
        if (inProcess.contains(lv)) {
            return new Solution(null, Solution.Status.CYCLE);
        }
        Solution result = null;
        inProcess.add(lv);
        Set<ConstraintExpression> rValues = constraints.get(lv);
        if (rValues == null) {
            return new Solution(null, Solution.Status.NOT_BOUND);
        }
        for (ConstraintExpression ce : rValues) {
            Solution solution = solve(ce, inProcess);
            if (solution != null && solution.status == Solution.Status.FOUND) {
                result = solution;
                break;
            }
        }
        if (result != null) {
            rValues.clear();
            rValues.add(new ConstantExpression(result.value));
        }
        inProcess.remove(lv);
        return result;
    }

    public Object getValue(LValueConstraintExpression lValue) {
        Set<ConstraintExpression> rValues = constraints.get(lValue);
        if (rValues == null) {
            return null;
        }
        if (rValues.size() != 1) {
            return null;
        }
        for (ConstraintExpression ce : rValues) {
            if (ce.getTag() == ConstraintExpression.ExpressionTag.CONSTANT) {
                return ((ConstantExpression) ce).getValue();
            }
        }
        return null;
    }

    private static class Solution {
        enum Status {
            FOUND,
            CYCLE,
            NOT_BOUND,
        }

        final Object value;
        final Status status;

        public Solution(Object value, Status status) {
            this.value = value;
            this.status = status;
        }
    }
}
