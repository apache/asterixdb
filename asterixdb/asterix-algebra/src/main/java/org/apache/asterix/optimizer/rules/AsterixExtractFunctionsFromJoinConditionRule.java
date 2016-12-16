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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.rewriter.rules.ExtractFunctionsFromJoinConditionRule;

public class AsterixExtractFunctionsFromJoinConditionRule extends ExtractFunctionsFromJoinConditionRule {

    @Override
    protected boolean processArgumentsToFunction(FunctionIdentifier fi) {
        return fi.equals(BuiltinFunctions.GET_ITEM);
    }

    @Override
    protected boolean isComparisonFunction(FunctionIdentifier fi) {
        return BuiltinFunctions.isSimilarityFunction(fi);
    }

}
