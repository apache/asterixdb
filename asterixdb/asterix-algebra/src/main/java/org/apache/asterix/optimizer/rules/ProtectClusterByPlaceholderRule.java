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
package org.apache.asterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Shields the CLUSTER BY assignment-centroid placeholder from variable inlining. The translator binds the
 * variable to a placeholder expression below the operator so the members nested plan can reference the row's
 * assignment centroid; {@code RewriteClusterByToKMeansRule} later replaces the placeholder with the real per-row
 * value. Inlining the placeholder's definition into those references before expansion would dissolve the
 * indirection the expansion rewires, so the variable is registered as not-to-be-inlined, which both
 * {@code InlineVariablesRule} and {@code RemoveRedundantVariablesRule} honor. The rule never changes the
 * plan; it only records the variable, and only when a CLUSTER BY operator is present.
 */
public class ProtectClusterByPlaceholderRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.CLUSTER_BY) {
            return false;
        }
        LogicalVariable assignedCentroid = ((ClusterByOperator) op).getAssignedCentroidVariable();
        if (assignedCentroid != null) {
            context.addNotToBeInlinedVar(assignedCentroid);
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
