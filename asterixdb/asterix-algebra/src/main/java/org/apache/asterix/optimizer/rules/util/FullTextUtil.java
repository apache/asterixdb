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
package org.apache.asterix.optimizer.rules.util;

import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.am.IOptimizableFuncExpr;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsFunctionDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FullTextUtil {

    public static boolean isFullTextContainsFunctionExpr(IOptimizableFuncExpr expr) {
        return isFullTextContainsFunctionExpr(expr.getFuncExpr());
    }

    public static boolean isFullTextContainsFunctionExpr(AbstractFunctionCallExpression expr) {
        FunctionIdentifier funcId = expr.getFunctionIdentifier();
        if (funcId.equals(BuiltinFunctions.FULLTEXT_CONTAINS)
                || funcId.equals(BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION)) {
            return true;
        }
        return false;
    }

    // If not a full-text function expression, then return null
    // Otherwise, return the full-text config if one exists in the expression, otherwise return the default config
    public static String getFullTextConfigNameFromExpr(IOptimizableFuncExpr expr) {
        return getFullTextConfigNameFromExpr(expr.getFuncExpr());
    }

    // ToDo: here we are parsing the expr manually, maybe we can find a better way to parse the arguments,
    //  e.g. convert the argument into an AdmObjectNode and then read from the object node
    public static String getFullTextConfigNameFromExpr(AbstractFunctionCallExpression funcExpr) {
        if (isFullTextContainsFunctionExpr(funcExpr) == false) {
            return null;
        }

        String configName = null;
        List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();

        // The first two arguments are
        // 1) the full-text record field to be queried,
        // 2) the query keyword array
        // The next fields are the list of full-text search options,
        // say, the next 4 fields can be "mode", "all", "config", "my_full_text_config"
        // Originally, the full-text search option is an Asterix record such as
        //     {"mode": "all", "config": "my_full_text_config"}
        for (int i = 2; i < arguments.size(); i += 2) {
            // The the full-text search option arguments are already checked in FullTextContainsParameterCheckAndSetRule,
            String optionName = ConstantExpressionUtil.getStringConstant(arguments.get(i).getValue());

            if (optionName.equalsIgnoreCase(FullTextContainsFunctionDescriptor.FULLTEXT_CONFIG_OPTION)) {
                configName = ConstantExpressionUtil.getStringConstant(arguments.get(i + 1).getValue());
                break;
            }
        }

        return configName;
    }

    public static InvertedIndexAccessMethod.SearchModifierType getFullTextSearchModeFromExpr(
            AbstractFunctionCallExpression funcExpr) {

        // After the third argument, the following arguments are full-text search options.
        for (int i = 2; i < funcExpr.getArguments().size(); i = i + 2) {
            String optionName = ConstantExpressionUtil.getStringArgument(funcExpr, i);

            if (optionName.equals(FullTextContainsFunctionDescriptor.SEARCH_MODE_OPTION)) {
                String searchType = ConstantExpressionUtil.getStringArgument(funcExpr, i + 1);

                if (searchType.equals(FullTextContainsFunctionDescriptor.SearchMode.ALL.getValue())) {
                    return InvertedIndexAccessMethod.SearchModifierType.CONJUNCTIVE;
                } else {
                    return InvertedIndexAccessMethod.SearchModifierType.DISJUNCTIVE;
                }
            }
        }

        // Use CONJUNCTIVE by default
        return InvertedIndexAccessMethod.SearchModifierType.CONJUNCTIVE;
    }

}
