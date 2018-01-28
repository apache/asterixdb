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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * If the first rule in the given collection is fired during the first iteration, it also runs the other rules
 * sequentially (round-robin) until one iteration over all rules produces no change. Except the case where the first
 * rule in the first iteration fails, all rules will be checked for each iteration.
 * An example scenario:
 * Suppose there are three rules - R1, R2, and R3.
 * During the first iteration, if R1, the first rule, is not fired, then R2 and R3 will not be checked.
 * If R1, the first rule, is fired, then R2 and R3 will be checked. Since the first iteration returns at least one true
 * (the first rule), there will be an another iteration. In the second iteration, we don't care whether R1 is fired or
 * not. This enforcement of 'first rule returns true' check is only executed in the first iteration.
 * So, if any of rules in the collection (R1, R2, and R3) is fired, then there will be another iteration(s) until
 * an iteration doesn't produce any change (no true from all rules).
 */
public class SequentialFirstRuleCheckFixpointRuleController extends AbstractRuleController {

    private boolean fullDfs;

    public SequentialFirstRuleCheckFixpointRuleController(boolean fullDfs) {
        super();
        this.fullDfs = fullDfs;
    }

    @Override
    public boolean rewriteWithRuleCollection(Mutable<ILogicalOperator> root,
            Collection<IAlgebraicRewriteRule> ruleCollection) throws AlgebricksException {
        List<IAlgebraicRewriteRule> rules;

        // This rule controller can only be applied for a list since it needs to enforce the "first" rule check.
        if (ruleCollection instanceof List) {
            rules = (List<IAlgebraicRewriteRule>) ruleCollection;
        } else {
            throw AlgebricksException.create(ErrorCode.RULECOLLECTION_NOT_INSTANCE_OF_LIST, this.getClass().getName());
        }

        if (rules.isEmpty()) {
            return false;
        }

        boolean anyRuleFired = false;
        boolean anyChange;
        boolean firstRuleChecked = false;
        do {
            anyChange = false;
            for (int i = 0; i < rules.size(); i++) {
                boolean ruleFired = rewriteOperatorRef(root, rules.get(i), true, fullDfs);
                // If the first rule returns false in the first iteration, stops applying the rules at all.
                if (!firstRuleChecked && i == 0 && !ruleFired) {
                    return ruleFired;
                }
                if (ruleFired) {
                    anyChange = true;
                    anyRuleFired = true;
                }
            }
            firstRuleChecked = true;
        } while (anyChange);

        return anyRuleFired;
    }
}
