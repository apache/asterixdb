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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectBlock implements Clause {

    private SelectClause selectClause;
    private FromClause fromClause;
    private List<LetClause> letClauses;
    private WhereClause whereClause;
    private GroupbyClause groupbyClause;
    private List<LetClause> letClausesAfterGby;
    private HavingClause havingClause;

    public SelectBlock(SelectClause selectClause, FromClause fromClause, List<LetClause> letClauses,
            WhereClause whereClause, GroupbyClause groupbyClause, List<LetClause> letClausesAfterGby,
            HavingClause havingClause) {
        this.selectClause = selectClause;
        this.fromClause = fromClause;
        this.letClauses = letClauses;
        this.whereClause = whereClause;
        this.groupbyClause = groupbyClause;
        this.havingClause = havingClause;
        this.letClausesAfterGby = letClausesAfterGby;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
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

    public List<LetClause> getLetList() {
        return letClauses;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    public GroupbyClause getGroupbyClause() {
        return groupbyClause;
    }

    public HavingClause getHavingClause() {
        return havingClause;
    }

    public boolean hasFromClause() {
        return fromClause != null;
    }

    public boolean hasLetClauses() {
        return letClauses != null && letClauses.size() > 0;
    }

    public boolean hasWhereClause() {
        return whereClause != null;
    }

    public boolean hasGroupbyClause() {
        return groupbyClause != null;
    }

    public boolean hasLetClausesAfterGroupby() {
        return letClausesAfterGby != null && letClausesAfterGby.size() > 0;
    }

    public List<LetClause> getLetListAfterGroupby() {
        return letClausesAfterGby;
    }

    public boolean hasHavingClause() {
        return havingClause != null;
    }
}
