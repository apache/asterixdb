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
package org.apache.asterix.lang.common.statement;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.lang3.ObjectUtils;

public class Query implements Statement {
    private final boolean explain;
    private boolean topLevel = true;
    private Expression body;
    private int varCounter;
    private List<String> dataverses = new ArrayList<>();
    private List<String> datasets = new ArrayList<>();

    public Query(boolean explain) {
        this.explain = explain;
    }

    public Query(boolean explain, boolean topLevel, Expression body, int varCounter, List<String> dataverses,
            List<String> datasets) {
        this.explain = explain;
        this.topLevel = topLevel;
        this.body = body;
        this.varCounter = varCounter;
        this.dataverses.addAll(dataverses);
        this.datasets.addAll(datasets);
    }

    public Expression getBody() {
        return body;
    }

    public void setBody(Expression body) {
        this.body = body;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public void setVarCounter(int varCounter) {
        this.varCounter = varCounter;
    }

    public void setTopLevel(boolean topLevel) {
        this.topLevel = topLevel;
    }

    public boolean isTopLevel() {
        return topLevel;
    }

    public boolean isExplain() {
        return explain;
    }

    @Override
    public byte getKind() {
        return Statement.Kind.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public void setDataverses(List<String> dataverses) {
        this.dataverses = dataverses;
    }

    public void setDatasets(List<String> datasets) {
        this.datasets = datasets;
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(body, datasets, dataverses, topLevel, explain);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Query)) {
            return false;
        }
        Query target = (Query) object;
        return explain == target.explain && ObjectUtils.equals(body, target.body)
                && ObjectUtils.equals(datasets, target.datasets) && ObjectUtils.equals(dataverses, target.dataverses)
                && topLevel == target.topLevel;
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }
}
