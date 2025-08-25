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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

/**
 * Visitor that validates SET expressions in UPDATE statements to prevent modification of primary key fields.
 *
 * <p>This visitor traverses UPDATE statements and checks all SET clause assignments to ensure
 * that no primary key fields are being modified.
 *
 * <p><b>Example - Invalid UPDATE ("id" is the primary key):</b>
 * UPDATE users AS u
 * SET u.id = 2, u.name = "John"
 * WHERE u.id = 1
 * This will throw a CompilationException: "Cannot set primary key field: u.id"
 *
 * <p><b>Example - Nested field path ("info.id" is the primary key)::</b>
 * UPDATE users AS u
 * SET u.info.id = "12345";
 *
 * This will also throw a CompilationException.
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.sqlpp.expression.SetExpression;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class CheckUpdateSetExpressionsVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final MetadataProvider metadataProvider;
    private String datasetName;
    private Namespace namespace;

    public CheckUpdateSetExpressionsVisitor(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(UpdateStatement updateStatement, ILangExpression arg) throws CompilationException {
        this.datasetName = updateStatement.getDatasetName();
        this.namespace = updateStatement.getNamespace();
        return super.visit(updateStatement, arg);
    }

    @Override
    public Expression visit(SetExpression setExpr, ILangExpression arg) throws CompilationException {
        List<Expression> pathExprList = setExpr.getPathExprList();
        List<String> fieldPath = new ArrayList<>();
        for (int i = 0; i < pathExprList.size(); i++) {
            Expression pathExpr = pathExprList.get(i);
            try {
                if (isPrimaryKeyField(pathExpr, fieldPath)) {
                    throw new CompilationException("Cannot set primary key field: " + pathExpr.toString());
                }
            } catch (AlgebricksException e) {
                throw new RuntimeException(e);
            }
        }
        return setExpr;
    }

    public boolean isPrimaryKeyField(Expression pathExp, List<String> fieldPath) throws AlgebricksException {
        if (pathExp.getKind() != Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
            return false;
        }
        fieldPath.clear();
        FieldAccessor fieldAccessor = (FieldAccessor) pathExp;
        // Extract the complete field path (handles nested fields like fieldX.pk)
        FieldAccessor current = fieldAccessor;
        while (true) {
            fieldPath.add(0, current.getIdent().getValue());
            Expression expr = current.getExpr();
            if (expr.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                current = (FieldAccessor) expr;
            } else if (expr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                // Found the base variable, now check primary keys
                DataverseName dataverseName = namespace.getDataverseName();
                String databaseName = namespace.getDatabaseName();
                Dataset dataset = metadataProvider.findDataset(databaseName, dataverseName, datasetName, false);
                return dataset != null && dataset.getPrimaryKeys().contains(fieldPath);
            } else {
                break;
            }
        }
        return false;
    }
}
