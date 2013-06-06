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

package edu.uci.ics.asterix.algebra.operators;

import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractExtensibleLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorExtension;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public class CommitOperator extends AbstractExtensibleLogicalOperator {

    private final List<LogicalVariable> primaryKeyLogicalVars;

    public CommitOperator(List<LogicalVariable> primaryKeyLogicalVars) {
        this.primaryKeyLogicalVars = primaryKeyLogicalVars;
    }

    @Override
    public boolean isMap() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public IOperatorExtension newInstance() {
        return new CommitOperator(primaryKeyLogicalVars);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        // TODO Auto-generated method stub
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
