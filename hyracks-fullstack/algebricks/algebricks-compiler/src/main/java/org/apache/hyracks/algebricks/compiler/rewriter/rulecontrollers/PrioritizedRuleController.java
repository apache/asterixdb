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

/**
 *
 * Runs each rule until it produces no changes. Then the whole collection of
 * rules is run again until no change is made.
 *
 *
 * @author Nicola
 *
 */

public class PrioritizedRuleController extends AbstractRuleController {

    public PrioritizedRuleController() {
        super();
    }

    @Override
    public boolean rewriteWithRuleCollection(Mutable<ILogicalOperator> root, Collection<IAlgebraicRewriteRule> rules)
            throws AlgebricksException {
        boolean anyRuleFired = false;
        boolean anyChange = false;
        do {
            anyChange = false;
            for (IAlgebraicRewriteRule r : rules) {
                while (true) {
                    boolean ruleFired = rewriteOperatorRef(root, r);
                    if (ruleFired) {
                        anyChange = true;
                        anyRuleFired = true;
                    } else {
                        break; // go to next rule
                    }
                }
            }
        } while (anyChange);
        return anyRuleFired;
    }
}
