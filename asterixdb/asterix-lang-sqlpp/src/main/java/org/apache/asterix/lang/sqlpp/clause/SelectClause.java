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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectClause extends AbstractClause {

    private SelectElement selectElement;
    private SelectRegular selectRegular;
    private boolean distinct;

    public SelectClause(SelectElement selectElement, SelectRegular selectRegular, boolean distinct) {
        this.selectElement = selectElement;
        this.selectRegular = selectRegular;
        this.distinct = distinct;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_CLAUSE;
    }

    public SelectElement getSelectElement() {
        return selectElement;
    }

    public SelectRegular getSelectRegular() {
        return selectRegular;
    }

    public boolean selectElement() {
        return selectElement != null;
    }

    public boolean selectRegular() {
        return selectRegular != null;
    }

    public boolean distinct() {
        return distinct;
    }

    @Override
    public String toString() {
        return "select " + (distinct ? "distinct " : "")
                + (selectElement() ? "element " + selectElement : String.valueOf(selectRegular));
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinct, selectElement, selectRegular);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectClause)) {
            return false;
        }
        SelectClause target = (SelectClause) object;
        return distinct == target.distinct && Objects.equals(selectElement, target.selectElement)
                && Objects.equals(selectRegular, target.selectRegular);
    }
}
