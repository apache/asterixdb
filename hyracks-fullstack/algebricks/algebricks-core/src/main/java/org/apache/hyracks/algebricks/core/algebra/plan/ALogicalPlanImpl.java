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
package org.apache.hyracks.algebricks.core.algebra.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;

public class ALogicalPlanImpl implements ILogicalPlan {
    private List<Mutable<ILogicalOperator>> roots;

    public ALogicalPlanImpl() {
        this.roots = new ArrayList<>();
    }

    public ALogicalPlanImpl(List<Mutable<ILogicalOperator>> roots) {
        this.roots = roots;
    }

    public ALogicalPlanImpl(Mutable<ILogicalOperator> root) {
        roots = new ArrayList<>(1);
        roots.add(root);
    }

    @Override
    public List<Mutable<ILogicalOperator>> getRoots() {
        return roots;
    }

    public void setRoots(List<Mutable<ILogicalOperator>> roots) {
        this.roots = roots;
    }

    public static String prettyPrintPlan(ILogicalPlan plan) throws AlgebricksException {
        return PlanPrettyPrinter.createStringPlanPrettyPrinter().printPlan(plan).toString();
    }

    @Override
    public String toString() {
        try {
            return ALogicalPlanImpl.prettyPrintPlan(this);
        } catch (AlgebricksException e) {
            throw new IllegalStateException(e);
        }
    }
}
