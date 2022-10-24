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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectClause extends AbstractClause {
    private final List<List<String>> fieldExclusions = new ArrayList<>();
    private SelectElement selectElement;
    private SelectRegular selectRegular;
    private boolean distinct;

    public SelectClause(SelectElement selectElement, SelectRegular selectRegular, List<List<String>> fieldExclusions,
            boolean distinct) {
        if (selectElement != null && selectRegular != null) {
            throw new IllegalArgumentException("SELECT-ELEMENT and SELECT-REGULAR cannot both be specified.");
        }
        if (selectElement != null && fieldExclusions != null && !fieldExclusions.isEmpty()) {
            throw new IllegalArgumentException("SELECT-ELEMENT and EXCLUDE cannot both be specified.");
        }

        this.selectElement = selectElement;
        this.selectRegular = selectRegular;
        this.distinct = distinct;
        if (fieldExclusions != null) {
            this.fieldExclusions.addAll(fieldExclusions);
        }
    }

    public SelectClause(SelectElement selectElement, SelectRegular selectRegular, boolean distinct) {
        this(selectElement, selectRegular, null, distinct);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_CLAUSE;
    }

    public void setSelectElement(SelectElement selectElement) throws CompilationException {
        if (!fieldExclusions.isEmpty() && selectElement != null) {
            // We forbid SELECT VALUE and EXCLUDE at the parser, so we should never reach here.
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, getSourceLocation(),
                    "SELECT ELEMENT and EXCLUDE cannot coexist!");
        }
        this.selectElement = selectElement;
        this.selectRegular = null;
    }

    public void setSelectRegular(SelectRegular selectRegular) {
        this.selectRegular = selectRegular;
        this.selectElement = null;
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

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public List<List<String>> getFieldExclusions() {
        return fieldExclusions;
    }

    @Override
    public String toString() {
        String distinctString = distinct ? "distinct " : "";
        String valueString = selectElement() ? ("element " + selectElement) : String.valueOf(selectRegular);
        String exceptString = "";
        if (!fieldExclusions.isEmpty()) {
            final Function<List<String>, String> fieldBuilder = f -> String.join(".", f);
            exceptString = " exclude " + fieldExclusions.stream().map(fieldBuilder).collect(Collectors.joining(", "));
        }
        return String.format("select %s%s%s", distinctString, valueString, exceptString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(distinct, selectElement, selectRegular, fieldExclusions);
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
                && Objects.equals(selectRegular, target.selectRegular)
                && Objects.equals(fieldExclusions, target.fieldExclusions);
    }
}
