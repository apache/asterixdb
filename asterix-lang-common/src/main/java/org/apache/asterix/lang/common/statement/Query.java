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

public class Query implements Statement {
    private boolean topLevel = true;
    private Expression body;
    private int varCounter;
    private List<String> dataverses = new ArrayList<String>();
    private List<String> datasets = new ArrayList<String>();

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

    @Override
    public Kind getKind() {
        return Kind.QUERY;
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
}
