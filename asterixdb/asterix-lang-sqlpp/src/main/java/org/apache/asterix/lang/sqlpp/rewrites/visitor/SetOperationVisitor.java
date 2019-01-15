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
import java.util.Collections;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This visitor rewrites set operation queries with order by and limit into
 * a nested query where the set operation part is a subquery in the from clause.
 * In this way, there is no special variable scoping mechanism that is needed
 * for order by and limit clauses after the set operation.
 */
/*
For example, the following query

SELECT ... FROM ...
UNION ALL
SELECT ... FROM ...
ORDER BY foo
Limit 5;

is rewritten into the following form:

SELECT VALUE v
FROM (
   SELECT ... FROM ...
   UNION ALL
   SELECT ... FROM ...
 ) AS v
ORDER BY foo
LIMIT 5;
*/
public class SetOperationVisitor extends AbstractSqlppExpressionScopingVisitor {

    public SetOperationVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        // Recursively visit nested select expressions.
        SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
        if (!selectSetOperation.hasRightInputs() || !(selectExpression.hasOrderby() || selectExpression.hasLimit())) {
            return super.visit(selectExpression, arg);
        }
        OrderbyClause orderBy = selectExpression.getOrderbyClause();
        LimitClause limit = selectExpression.getLimitClause();

        SourceLocation sourceLoc = selectExpression.getSourceLocation();

        // Wraps the set operation part with a subquery.
        SelectExpression nestedSelectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        nestedSelectExpression.setSourceLocation(sourceLoc);
        VariableExpr newBindingVar = new VariableExpr(context.newVariable()); // Binding variable for the subquery.
        newBindingVar.setSourceLocation(sourceLoc);
        FromTerm newFromTerm = new FromTerm(nestedSelectExpression, newBindingVar, null, null);
        newFromTerm.setSourceLocation(sourceLoc);
        FromClause newFromClause = new FromClause(new ArrayList<>(Collections.singletonList(newFromTerm)));
        newFromClause.setSourceLocation(sourceLoc);
        SelectClause selectClause = new SelectClause(new SelectElement(newBindingVar), null, false);
        selectClause.setSourceLocation(sourceLoc);
        SelectBlock selectBlock = new SelectBlock(selectClause, newFromClause, null, null, null);
        selectBlock.setSourceLocation(sourceLoc);
        SelectSetOperation newSelectSetOperation =
                new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        newSelectSetOperation.setSourceLocation(sourceLoc);
        // Puts together the generated select-from-where query and order by/limit.
        SelectExpression newSelectExpression = new SelectExpression(selectExpression.getLetList(),
                newSelectSetOperation, orderBy, limit, selectExpression.isSubquery());
        newSelectExpression.setSourceLocation(sourceLoc);
        return super.visit(newSelectExpression, arg);
    }

}
