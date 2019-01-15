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

package org.apache.asterix.lang.sqlpp.clause;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectBlock extends AbstractClause {

    private SelectClause selectClause;
    private FromClause fromClause;
    private final List<AbstractClause> letWhereClauses = new ArrayList<>();
    private GroupbyClause groupbyClause;
    private final List<AbstractClause> letHavingClausesAfterGby = new ArrayList<>();

    public SelectBlock(SelectClause selectClause, FromClause fromClause, List<AbstractClause> letWhereClauses,
            GroupbyClause groupbyClause, List<AbstractClause> letHavingClausesAfterGby) {
        this.selectClause = selectClause;
        this.fromClause = fromClause;
        if (letWhereClauses != null) {
            this.letWhereClauses.addAll(letWhereClauses);
        }
        this.groupbyClause = groupbyClause;
        if (letHavingClausesAfterGby != null) {
            this.letHavingClausesAfterGby.addAll(letHavingClausesAfterGby);
        }
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_BLOCK;
    }

    public SelectClause getSelectClause() {
        return selectClause;
    }

    public FromClause getFromClause() {
        return fromClause;
    }

    public List<AbstractClause> getLetWhereList() {
        return letWhereClauses;
    }

    public GroupbyClause getGroupbyClause() {
        return groupbyClause;
    }

    public List<AbstractClause> getLetHavingListAfterGroupby() {
        return letHavingClausesAfterGby;
    }

    public boolean hasFromClause() {
        return fromClause != null;
    }

    public boolean hasLetWhereClauses() {
        return !letWhereClauses.isEmpty();
    }

    public boolean hasGroupbyClause() {
        return groupbyClause != null;
    }

    public boolean hasLetHavingClausesAfterGroupby() {
        return !letHavingClausesAfterGby.isEmpty();
    }

    public void setGroupbyClause(GroupbyClause groupbyClause) {
        this.groupbyClause = groupbyClause;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromClause, groupbyClause, letWhereClauses, letHavingClausesAfterGby, selectClause);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectBlock)) {
            return false;
        }
        SelectBlock target = (SelectBlock) object;
        return Objects.equals(fromClause, target.fromClause) && Objects.equals(groupbyClause, target.groupbyClause)
                && Objects.equals(letWhereClauses, target.letWhereClauses)
                && Objects.equals(letHavingClausesAfterGby, target.letHavingClausesAfterGby)
                && Objects.equals(selectClause, target.selectClause);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(selectClause);
        if (hasFromClause()) {
            sb.append(' ').append(fromClause);
        }
        if (hasLetWhereClauses()) {
            sb.append(' ').append(letWhereClauses);
        }
        if (hasGroupbyClause()) {
            sb.append(' ').append(groupbyClause);
        }
        if (hasLetHavingClausesAfterGroupby()) {
            sb.append(' ').append(letHavingClausesAfterGby);
        }
        return sb.toString();
    }
}
