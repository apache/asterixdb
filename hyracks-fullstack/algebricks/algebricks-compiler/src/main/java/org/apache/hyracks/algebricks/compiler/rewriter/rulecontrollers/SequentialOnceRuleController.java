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
package org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers;

import java.util.Collection;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class SequentialOnceRuleController extends AbstractRuleController {

    private final boolean enterNestedPlans;

    public SequentialOnceRuleController(boolean enterNestedPlans) {
        super();
        this.enterNestedPlans = enterNestedPlans;
    }

    @Override
    public boolean rewriteWithRuleCollection(Mutable<ILogicalOperator> root, Collection<IAlgebraicRewriteRule> rules)
            throws AlgebricksException {
        boolean fired = false;
        for (IAlgebraicRewriteRule rule : rules) {
            if (rewriteOperatorRef(root, rule, enterNestedPlans, true)) {
                fired = true;
            }
        }
        return fired;
    }
}
