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
package org.apache.asterix.lang.common.clause;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class GroupbyClause extends AbstractClause {

    private List<GbyVariableExpressionPair> gbyPairList;
    private List<GbyVariableExpressionPair> decorPairList;
    private Map<Expression, VariableExpr> withVarMap;
    private VariableExpr groupVar;
    private List<Pair<Expression, Identifier>> groupFieldList = new ArrayList<>();
    private boolean hashGroupByHint = false;
    private boolean groupAll = false;

    public GroupbyClause() {
        // Default constructor.
    }

    public GroupbyClause(List<GbyVariableExpressionPair> gbyPairList, List<GbyVariableExpressionPair> decorPairList,
            Map<Expression, VariableExpr> withVarList, VariableExpr groupVarExpr,
            List<Pair<Expression, Identifier>> groupFieldList, boolean hashGroupByHint) {
        this(gbyPairList, decorPairList, withVarList, groupVarExpr, groupFieldList, hashGroupByHint, false);
    }

    public GroupbyClause(List<GbyVariableExpressionPair> gbyPairList, List<GbyVariableExpressionPair> decorPairList,
            Map<Expression, VariableExpr> withVarList, VariableExpr groupVarExpr,
            List<Pair<Expression, Identifier>> groupFieldList, boolean hashGroupByHint, boolean groupAll) {
        this.gbyPairList = gbyPairList;
        this.decorPairList = decorPairList;
        this.withVarMap = withVarList;
        this.groupVar = groupVarExpr;
        if (groupFieldList != null) {
            this.groupFieldList = groupFieldList;
        }
        this.hashGroupByHint = hashGroupByHint;
        this.groupAll = groupAll;
    }

    public List<GbyVariableExpressionPair> getGbyPairList() {
        return gbyPairList;
    }

    public void setGbyPairList(List<GbyVariableExpressionPair> vePairList) {
        this.gbyPairList = vePairList;
    }

    public Map<Expression, VariableExpr> getWithVarMap() {
        return withVarMap;
    }

    public void setWithVarMap(Map<Expression, VariableExpr> withVarList) {
        this.withVarMap = withVarList;
    }

    public VariableExpr getGroupVar() {
        return groupVar;
    }

    public void setGroupVar(VariableExpr groupVarExpr) {
        this.groupVar = groupVarExpr;
    }

    public List<Pair<Expression, Identifier>> getGroupFieldList() {
        return groupFieldList;
    }

    public void setGroupFieldList(List<Pair<Expression, Identifier>> groupFieldList) {
        this.groupFieldList = groupFieldList;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.GROUP_BY_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public void setDecorPairList(List<GbyVariableExpressionPair> decorPairList) {
        this.decorPairList = decorPairList;
    }

    public List<GbyVariableExpressionPair> getDecorPairList() {
        return decorPairList;
    }

    public void setHashGroupByHint(boolean hashGroupByHint) {
        this.hashGroupByHint = hashGroupByHint;
    }

    public boolean hasHashGroupByHint() {
        return hashGroupByHint;
    }

    public boolean hasDecorList() {
        return decorPairList != null && !decorPairList.isEmpty();
    }

    public boolean hasWithMap() {
        return withVarMap != null && !withVarMap.isEmpty();
    }

    public boolean hasGroupVar() {
        return groupVar != null;
    }

    public boolean hasGroupFieldList() {
        return groupFieldList != null && !groupFieldList.isEmpty();
    }

    public boolean isGroupAll() {
        return groupAll;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decorPairList, gbyPairList, groupAll, groupFieldList, groupVar, hashGroupByHint,
                withVarMap);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GroupbyClause)) {
            return false;
        }
        GroupbyClause target = (GroupbyClause) object;
        return Objects.equals(decorPairList, target.decorPairList) && Objects.equals(gbyPairList, target.gbyPairList)
                && groupAll == target.groupAll && Objects.equals(groupFieldList, target.groupFieldList)
                && Objects.equals(groupVar, target.groupVar) && hashGroupByHint == target.hashGroupByHint
                && Objects.equals(withVarMap, target.withVarMap);
    }
}
