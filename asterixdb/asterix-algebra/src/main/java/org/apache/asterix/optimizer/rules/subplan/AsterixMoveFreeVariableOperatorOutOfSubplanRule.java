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
package org.apache.asterix.optimizer.rules.subplan;

import java.util.Set;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.rewriter.rules.subplan.MoveFreeVariableOperatorOutOfSubplanRule;

public class AsterixMoveFreeVariableOperatorOutOfSubplanRule extends MoveFreeVariableOperatorOutOfSubplanRule {

    @Override
    protected boolean movableOperatorKind(LogicalOperatorTag operatorTag) {
        return (operatorTag == LogicalOperatorTag.ASSIGN);
    }

    @Override
    protected boolean movableIndependentOperator(ILogicalOperator op, Set<LogicalVariable> usedVars) {
        return !usedVars.isEmpty();
    }
}
