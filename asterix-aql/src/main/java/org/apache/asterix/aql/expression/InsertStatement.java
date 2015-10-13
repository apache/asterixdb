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
package org.apache.asterix.aql.expression;

import java.util.List;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class InsertStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier datasetName;
    private final Query query;
    private final int varCounter;
    private final boolean returnRecord;
    private final List<String> returnField;

    public InsertStatement(Identifier dataverseName, Identifier datasetName, Query query, int varCounter,
            boolean returnRecord, List<String> returnField) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.query = query;
        this.varCounter = varCounter;
        this.returnField = returnField;
        this.returnRecord = returnRecord;
    }

    @Override
    public Kind getKind() {
        return Kind.INSERT;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public boolean getReturnRecord() {
        return returnRecord;
    }

    public List<String> getReturnField() {
        return returnField;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitInsertStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
