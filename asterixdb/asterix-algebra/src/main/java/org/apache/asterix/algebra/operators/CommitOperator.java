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

package org.apache.asterix.algebra.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDelegatedLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorDelegate;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public class CommitOperator extends AbstractDelegatedLogicalOperator {

    private List<LogicalVariable> primaryKeyLogicalVars;
    private boolean isSink;

    public CommitOperator(boolean isSink) {
        this.isSink = isSink;
        primaryKeyLogicalVars = new ArrayList<>();
    }

    public CommitOperator(List<LogicalVariable> primaryKeyLogicalVars, boolean isSink) {
        this.primaryKeyLogicalVars = primaryKeyLogicalVars;
        this.isSink = isSink;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    public boolean isSink() {
        return isSink;
    }

    public void setSink(boolean isSink) {
        this.isSink = isSink;
    }

    //Provided for Extensions but not used by core
    public void setVars(List<LogicalVariable> primaryKeyLogicalVars) {
        this.primaryKeyLogicalVars = primaryKeyLogicalVars;
    }

    @Override
    public IOperatorDelegate newInstance() {
        return new CommitOperator(primaryKeyLogicalVars, isSink);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public String toString() {
        return "commit";
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> usedVars) {
        usedVars.addAll(primaryKeyLogicalVars);
    }

    @Override
    public void getProducedVariables(Collection<LogicalVariable> producedVars) {
        // No produced variables.
    }
}
