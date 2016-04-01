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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupBySugarVisitor;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;

public class SqlppRewriteUtil {

    // Applying sugar rewriting for group-by.
    public static Expression rewriteExpressionUsingGroupVariable(VariableExpr groupVar,
            Collection<VariableExpr> targetVarList, ILangExpression expr, LangRewritingContext context)
                    throws AsterixException {
        SqlppGroupBySugarVisitor visitor = new SqlppGroupBySugarVisitor(context, groupVar, targetVarList);
        return expr.accept(visitor, null);
    }

    public static Set<VariableExpr> getFreeVariable(Expression expr) throws AsterixException {
        Set<VariableExpr> vars = new HashSet<>();
        FreeVariableVisitor visitor = new FreeVariableVisitor();
        expr.accept(visitor, vars);
        return vars;
    }

    public static ILangExpression deepCopy(ILangExpression expr) throws AsterixException {
        if (expr == null) {
            return expr;
        }
        DeepCopyVisitor visitor = new DeepCopyVisitor();
        return expr.accept(visitor, null);
    }

}
