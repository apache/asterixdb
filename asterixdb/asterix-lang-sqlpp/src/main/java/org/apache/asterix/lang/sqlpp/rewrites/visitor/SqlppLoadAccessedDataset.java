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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.CopyToStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.metadata.entities.EntityDetails;
import org.apache.asterix.om.functions.BuiltinFunctions;

public class SqlppLoadAccessedDataset extends AbstractSqlppSimpleExpressionVisitor {

    protected final LangRewritingContext context;

    public SqlppLoadAccessedDataset(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(CopyToStatement stmtCopy, ILangExpression arg) throws CompilationException {
        ExternalDetailsDecl externalDetailsDecl = stmtCopy.getExternalDetailsDecl();
        Map<String, String> properties = externalDetailsDecl.getProperties();
        String databaseName = properties.getOrDefault("database", "Default");
        String dataverseNameString = properties.getOrDefault("dataverse", "Default");
        String linkName = properties.getOrDefault("name", "");
        DataverseName dataverseName;
        try {
            dataverseName = DataverseName.createFromCanonicalForm(dataverseNameString);
        } catch (AsterixException e) {
            throw new IllegalStateException(e);
        }

        context.getMetadataProvider().addAccessedEntity(
                new EntityDetails(databaseName, dataverseName, linkName, EntityDetails.EntityType.LINK));
        return super.visit(stmtCopy, arg);
    }

    @Override
    public Expression visit(CallExpr expression, ILangExpression arg) {
        if (BuiltinFunctions.DATASET.equals(expression.getFunctionSignature().createFunctionIdentifier())) {
            List<Expression> exprs = expression.getExprList();
            String databaseName, dataverseNameArg, datasetName;
            EntityDetails.EntityType entityType;
            entityType = exprs.size() > 3 && Boolean.TRUE.equals(ExpressionUtils.getBooleanLiteral(exprs.get(3)))
                    ? EntityDetails.EntityType.VIEW : EntityDetails.EntityType.DATASET;

            databaseName = ExpressionUtils.getStringLiteral(exprs.get(0));
            dataverseNameArg = ExpressionUtils.getStringLiteral(exprs.get(1));
            DataverseName dataverseName;
            try {
                dataverseName = DataverseName.createFromCanonicalForm(dataverseNameArg);
            } catch (AsterixException e) {
                throw new IllegalStateException(e);
            }
            datasetName = ExpressionUtils.getStringLiteral(exprs.get(2));

            context.getMetadataProvider()
                    .addAccessedEntity(new EntityDetails(databaseName, dataverseName, datasetName, entityType));
        } else {
            FunctionSignature signature = expression.getFunctionSignature();
            String functionName = signature.getName() + "(" + signature.getArity() + ")";
            context.getMetadataProvider().addAccessedEntity(new EntityDetails(signature.getDatabaseName(),
                    signature.getDataverseName(), functionName, EntityDetails.EntityType.FUNCTION));
        }
        return expression;
    }
}
