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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class GroupbyClause implements Clause {

    private List<GbyVariableExpressionPair> gbyPairList;
    private List<GbyVariableExpressionPair> decorPairList;
    private List<VariableExpr> withVarList;
    private boolean hashGroupByHint;

    public GroupbyClause() {
    }

    public GroupbyClause(List<GbyVariableExpressionPair> gbyPairList, List<GbyVariableExpressionPair> decorPairList,
            List<VariableExpr> withVarList, boolean hashGroupByHint) {
        this.gbyPairList = gbyPairList;
        this.setDecorPairList(decorPairList);
        this.withVarList = withVarList;
        this.hashGroupByHint = hashGroupByHint;
    }

    public List<GbyVariableExpressionPair> getGbyPairList() {
        return gbyPairList;
    }

    public void setGbyPairList(List<GbyVariableExpressionPair> vePairList) {
        this.gbyPairList = vePairList;
    }

    public List<VariableExpr> getWithVarList() {
        return withVarList;
    }

    public void setWithVarList(List<VariableExpr> withVarList) {
        this.withVarList = withVarList;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.GROUP_BY_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
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
        return decorPairList != null && decorPairList.size() > 0;
    }

    public boolean hasWithList() {
        return withVarList != null && withVarList.size() > 0;
    }
}
