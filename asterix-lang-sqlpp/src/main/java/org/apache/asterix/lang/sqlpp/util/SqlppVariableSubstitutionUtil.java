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

import java.util.ArrayList;
import java.util.List;
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

    public static List<ILangExpression> substituteVariable(List<ILangExpression> expressions,
            VariableSubstitutionEnvironment env) throws AsterixException {
        SqlppCloneAndSubstituteVariablesVisitor visitor = new SqlppSubstituteVariablesVisitor(
                new LangRewritingContext(0));
        List<ILangExpression> newExprs = new ArrayList<ILangExpression>();
        for (ILangExpression expression : expressions) {
            newExprs.add(expression.accept(visitor, env).first);
        }
        return newExprs;
    }

    public static ILangExpression substituteVariable(ILangExpression expression, VariableSubstitutionEnvironment env)
            throws AsterixException {
        SqlppSubstituteVariablesVisitor visitor = new SqlppSubstituteVariablesVisitor(new LangRewritingContext(0));
        return expression.accept(visitor, env).first;
    }

    public static List<ILangExpression> substituteVariable(List<ILangExpression> expressions,
            Map<VariableExpr, Expression> varExprMap) throws AsterixException {
        SqlppSubstituteVariablesVisitor visitor = new SqlppSubstituteVariablesVisitor(new LangRewritingContext(0));
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment(varExprMap);
        List<ILangExpression> newExprs = new ArrayList<ILangExpression>();
        for (ILangExpression expression : expressions) {
            newExprs.add(expression.accept(visitor, env).first);
        }
        return newExprs;
    }

    public static ILangExpression substituteVariable(ILangExpression expression,
            Map<VariableExpr, Expression> varExprMap) throws AsterixException {
        SqlppSubstituteVariablesVisitor visitor = new SqlppSubstituteVariablesVisitor(new LangRewritingContext(0));
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment(varExprMap);
        return expression.accept(visitor, env).first;
    }

}
