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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.visitor.CheckSql92AggregateVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;

public class SqlppGlobalAggregationSugarVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws AsterixException {
        SelectClause selectClause = selectBlock.getSelectClause();
        if (!selectBlock.hasGroupbyClause() && selectBlock.hasFromClause()) {
            boolean addImplicitGby;
            if (selectClause.selectRegular()) {
                addImplicitGby = isSql92Aggregate(selectClause.getSelectRegular(), selectBlock);
            } else {
                addImplicitGby = isSql92Aggregate(selectClause.getSelectElement(), selectBlock);
            }
            if (addImplicitGby) {
                // Adds an implicit group-by clause for SQL-92 global aggregate.
                List<GbyVariableExpressionPair> gbyPairList = new ArrayList<>();
                List<GbyVariableExpressionPair> decorPairList = new ArrayList<>();
                GroupbyClause gbyClause = new GroupbyClause(gbyPairList, decorPairList, new HashMap<>(), null, null,
                        false, true);
                selectBlock.setGroupbyClause(gbyClause);
            }
        }
        return super.visit(selectBlock, arg);
    }

    private boolean isSql92Aggregate(ILangExpression expr, SelectBlock selectBlock) throws AsterixException {
        CheckSql92AggregateVisitor visitor = new CheckSql92AggregateVisitor();
        return expr.accept(visitor, selectBlock);
    }
}
