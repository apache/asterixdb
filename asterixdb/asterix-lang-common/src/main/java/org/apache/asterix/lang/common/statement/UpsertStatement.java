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

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;

public class UpsertStatement extends InsertStatement {

    public UpsertStatement(Identifier dataverseName, Identifier datasetName, Query query, int varCounter,
            VariableExpr var, Expression returnExpression) {
        super(dataverseName, datasetName, query, varCounter, var, returnExpression);
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.UPSERT;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Statement.Kind.UPSERT.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return this == object || object instanceof UpsertStatement && super.equals(object);
    }

}
