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
package org.apache.asterix.lang.sqlpp.util;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.sqlpp.visitor.SqlppCloneAndSubstituteVariablesVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteVariablesVisitor;

public class SqlppVariableSubstitutionUtil {

    /**
     * Substitute variables with corresponding expressions according to the varExprMap.
     * The substitution should be done BEFORE unique ids are assigned to variables.
     * In other words, when we call this method, ids of all variables are zero.
     *
     * @param expression,
     *            the expression for substituting variables.
     * @param varExprMap
     *            a map that maps variables to their corresponding expressions.
     * @return a new expression in which variables are substituted.
     * @throws AsterixException
     */
    public static ILangExpression substituteVariableWithoutContext(ILangExpression expression,
            Map<VariableExpr, Expression> varExprMap) throws AsterixException {
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment(varExprMap);
        return substituteVariableWithoutContext(expression, env);
    }

    /**
     * Substitute variables with corresponding expressions according to the varExprMap.
     * The substitution should be done BEFORE unique ids are assigned to variables.
     * In other words, when we call this method, ids of all variables are zero.
     *
     * @param expression,
     *            the expression for substituting variables.
     * @param env,
     *            internally contains a map that maps variables to their corresponding expressions.
     * @return a new expression in which variables are substituted.
     * @throws AsterixException
     */
    public static ILangExpression substituteVariableWithoutContext(ILangExpression expression,
            VariableSubstitutionEnvironment env) throws AsterixException {
        SqlppSubstituteVariablesVisitor visitor = new SqlppSubstituteVariablesVisitor();
        return expression.accept(visitor, env).first;
    }

    /**
     * Substitute variables with corresponding expressions according to the varExprMap.
     * The substitution should be done AFTER unique ids are assigned to different variables.
     *
     * @param expression,
     *            the expression for substituting variables.
     * @param varExprMap,
     *            a map that maps variables to their corresponding expressions.
     * @param context,
     *            manages the ids of variables so as to guarantee the uniqueness of ids for different variables (even with the same name).
     * @return a cloned new expression in which variables are substituted, and its bounded variables have new unique ids.
     * @throws AsterixException
     */
    public static ILangExpression cloneAndSubstituteVariable(ILangExpression expression,
            Map<VariableExpr, Expression> varExprMap, LangRewritingContext context) throws AsterixException {
        SqlppCloneAndSubstituteVariablesVisitor visitor = new SqlppCloneAndSubstituteVariablesVisitor(context);
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment(varExprMap);
        return expression.accept(visitor, env).first;
    }
}
