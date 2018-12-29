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
package org.apache.asterix.lang.common.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.CloneAndSubstituteVariablesVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class VariableCloneAndSubstitutionUtil {

    private VariableCloneAndSubstitutionUtil() {
    }

    public static List<GbyVariableExpressionPair> substInVarExprPair(LangRewritingContext context,
            List<GbyVariableExpressionPair> gbyVeList, VariableSubstitutionEnvironment newSubs,
            CloneAndSubstituteVariablesVisitor visitor) throws CompilationException {
        VariableSubstitutionEnvironment subs = newSubs;
        List<GbyVariableExpressionPair> veList = new LinkedList<>();
        for (GbyVariableExpressionPair vep : gbyVeList) {
            VariableExpr oldGbyVar = vep.getVar();
            VariableExpr newGbyVar = null;
            if (oldGbyVar != null) {
                newGbyVar = visitor.generateNewVariable(context, oldGbyVar);
                subs = eliminateSubstFromList(newGbyVar, subs);
            }
            Pair<ILangExpression, VariableSubstitutionEnvironment> p1 = vep.getExpr().accept(visitor, subs);
            GbyVariableExpressionPair ve2 = new GbyVariableExpressionPair(newGbyVar, (Expression) p1.first);
            veList.add(ve2);
        }
        return veList;
    }

    public static List<Pair<Expression, Identifier>> substInFieldList(List<Pair<Expression, Identifier>> fieldList,
            VariableSubstitutionEnvironment newSubs, CloneAndSubstituteVariablesVisitor visitor)
            throws CompilationException {
        List<Pair<Expression, Identifier>> newFieldList = new ArrayList<>(fieldList.size());
        for (Pair<Expression, Identifier> p : fieldList) {
            Expression newExpr = (Expression) p.first.accept(visitor, newSubs).first;
            newFieldList.add(new Pair<>(newExpr, p.second));
        }
        return newFieldList;
    }

    public static VariableSubstitutionEnvironment eliminateSubstFromList(VariableExpr variableExpr,
            VariableSubstitutionEnvironment arg) {
        VariableSubstitutionEnvironment newArg = new VariableSubstitutionEnvironment(arg);
        newArg.removeSubstitution(variableExpr);
        return newArg;
    }

    public static List<Expression> visitAndCloneExprList(List<Expression> oldExprList,
            VariableSubstitutionEnvironment arg, CloneAndSubstituteVariablesVisitor visitor)
            throws CompilationException {
        List<Expression> exprs = new ArrayList<>(oldExprList.size());
        for (Expression e : oldExprList) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p1 = e.accept(visitor, arg);
            exprs.add((Expression) p1.first);
        }
        return exprs;
    }
}
