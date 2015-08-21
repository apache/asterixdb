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
package org.apache.asterix.optimizer.rules;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractFunctionsFromJoinConditionRule;

public class AsterixExtractFunctionsFromJoinConditionRule extends ExtractFunctionsFromJoinConditionRule {

    @Override
    protected boolean processArgumentsToFunction(FunctionIdentifier fi) {
        return fi.equals(AsterixBuiltinFunctions.GET_ITEM);
    }

    @Override
    protected boolean isComparisonFunction(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.isSimilarityFunction(fi);
    }

}
