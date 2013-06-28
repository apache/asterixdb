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

package edu.uci.ics.asterix.optimizer.rules;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.PushFunctionsBelowJoin;

/**
 * Pushes similarity function-call expressions below a join if possible.
 * Assigns the similarity function-call expressions to new variables, and replaces the original
 * expression with a corresponding variable reference expression.
 * This rule can help reduce the cost of computing expensive similarity functions by pushing them below
 * a join (which may blow up the cardinality).
 * Also, this rule may help to enable other rules such as common subexpression elimination, again to reduce
 * the number of calls to expensive similarity functions.
 * 
 * Example:
 * 
 * Before plan:
 * assign [$$10] <- [funcA(funcB(simFuncX($$3, $$4)))]
 *   join (some condition) 
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       ...
 * 
 * After plan:
 * assign [$$10] <- [funcA(funcB($$11))]
 *   join (some condition) 
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       assign[$$11] <- [simFuncX($$3, $$4)]
 *         ...
 */
public class PushSimilarityFunctionsBelowJoin extends PushFunctionsBelowJoin {

    private static final Set<FunctionIdentifier> simFuncIdents = new HashSet<FunctionIdentifier>();
    static {
        simFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD);
        simFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
        simFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE);
        simFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK);
    }

    public PushSimilarityFunctionsBelowJoin() {
        super(simFuncIdents);
    }
}
