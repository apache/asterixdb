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
package org.apache.asterix.lang.sqlpp.expression;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.parser.SetExpressionTree;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * An UPDATE statement consists of one or more ChangeExpressions.
 *
 * <p>Each ChangeExpression represents a single change operation (SET, UPDATE, DELETE, or INSERT).
 * Multiple ChangeExpressions can be chained to perform a sequence of updates.
 * Each ChangeExpression transforms the data produced by its predecessor (priorExpr)
 * and passes the result to the next one through changeSeq.
 *
 * <p>Example:
 * <pre>
 *   UPDATE UserTypes AS u
 *     SET u.totalcost = 25
 *     SET u.totaltax  = u.totalcost * 1 / 100
 *   WHERE u.age = 23;
 * </pre>
 *
 * <p>This produces two ChangeExpressions:
 *      The first performs a SET operation on totalcost
 *      The second performs a SET operation on totaltax, using the
 *      updated totalcost from the previous change.
 */

public class ChangeExpression extends AbstractExpression {
    private List<Expression> valueExprs;
    private List<Expression> pathExprs;
    private Expression priorExpr;
    private final SetExpressionTree exprTree;
    private Expression dataTransformRecord;
    private Expression dataRemovalRecord;
    private Expression changeTargetExpr;
    private final VariableExpr aliasVar;
    private final VariableExpr posVar;
    private Expression changeSeq;
    private Expression condition;
    private final UpdateType type;
    private final Expression posExpr;
    private final Expression sourceExpr;

    public enum UpdateType {
        INSERT,
        DELETE,
        UPDATE
    }

    public ChangeExpression(Expression changeTargetExpr, VariableExpr aliasVar, VariableExpr posVar,
            Expression changeSeq, Expression condition, UpdateType type, Expression posExpr, Expression sourceExpr) {
        this.exprTree = new SetExpressionTree();
        //Needed when returning results from update.
        this.dataTransformRecord = null;
        this.dataRemovalRecord = null;
        this.changeTargetExpr = changeTargetExpr;
        this.aliasVar = aliasVar;
        this.posVar = posVar;
        this.changeSeq = changeSeq;
        this.condition = condition;
        this.type = type;
        this.posExpr = posExpr;
        this.sourceExpr = sourceExpr;
        this.valueExprs = null;
        this.pathExprs = null;
    }

    public ChangeExpression(List<Expression> valueExprs, List<Expression> pathExprs) {
        this(null, null, null, null, null, null, null, null);
        this.valueExprs = valueExprs;
        this.pathExprs = pathExprs;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE_CHANGE_EXPRESSION;
    }

    public Expression getDataTransformRecord() {
        return this.dataTransformRecord;
    }

    public Expression getDataRemovalRecord() {
        return this.dataRemovalRecord;
    }

    public List<Expression> getValueExprs() {
        return valueExprs;
    }

    public void setValueExprs(List<Expression> valueExprs) {
        this.valueExprs = valueExprs;
    }

    public List<Expression> getPathExprs() {
        return pathExprs;
    }

    public void setPathExprs(List<Expression> pathExprs) {
        this.pathExprs = pathExprs;
    }

    public Expression getPriorExpr() {
        return priorExpr;
    }

    public Expression getChangeTargetExpr() {
        return changeTargetExpr;
    }

    public VariableExpr getAliasVar() {
        return aliasVar;
    }

    public VariableExpr getPosVar() {
        return posVar;
    }

    public Expression getChangeSeq() {
        return changeSeq;
    }

    public Expression getCondition() {
        return condition;
    }

    public Expression getPosExpr() {
        return posExpr;
    }

    public Expression getSourceExpr() {
        return sourceExpr;
    }

    public void setChangeTargetExpr(Expression changeTargetExpr) {
        this.changeTargetExpr = changeTargetExpr;
    }

    public void setChangeSeq(Expression changeSeq) {
        this.changeSeq = changeSeq;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public void setPriorExpr(Expression priorExpr) {
        this.priorExpr = priorExpr;
    }

    public boolean hasPathValueExprs() {
        return pathExprs != null && valueExprs != null;
    }

    public void setDataTransformRecord(Expression dataTransformRecord) {
        this.dataTransformRecord = dataTransformRecord;
    }

    public boolean hasDataTransformRecord() {
        return this.dataTransformRecord != null;
    }

    public void setDataRemovalRecord(Expression dataRemovalRecord) {
        this.dataRemovalRecord = dataRemovalRecord;
    }

    public boolean hasDataRemovalRecord() {
        return this.dataRemovalRecord != null;
    }

    public boolean hasPriorExpr() {
        return this.priorExpr != null;
    }

    public boolean hasChangeTargetExpr() {
        return this.changeTargetExpr != null;
    }

    public boolean hasChangeSeq() {
        return this.changeSeq != null;
    }

    public boolean hasCondition() {
        return this.condition != null;
    }

    public UpdateType getType() {
        return type;
    }

    public void populateExprTree() throws CompilationException {
        if (getPathExprs() == null || getPathExprs().isEmpty()) {
            return;
        }
        List<Expression> currValueList = getValueExprs();
        List<Expression> currPathList = getPathExprs();
        for (int i = 0; i < currValueList.size(); i++) {
            exprTree.insertPath(currPathList.get(i), currValueList.get(i)); // throws exception if collides with a path

        }
    }

    public void createTransformationRecords() {
        if (exprTree.isEmpty()) {
            return;
        }
        Pair<Expression, Expression> result = exprTree.createRecordConstructor();
        if (result.first == null && result.second == null) {
            return;
        }
        dataTransformRecord = result.first;
        dataRemovalRecord = result.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathExprs, valueExprs, dataTransformRecord, dataRemovalRecord, exprTree, priorExpr,
                changeTargetExpr, aliasVar, posVar, changeSeq, condition, type, posExpr, sourceExpr);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ChangeExpression other)) {
            return false;
        }
        return Objects.equals(pathExprs, other.pathExprs) && Objects.equals(valueExprs, other.valueExprs)
                && Objects.equals(priorExpr, other.priorExpr) && Objects.equals(exprTree, other.exprTree)
                && Objects.equals(dataTransformRecord, other.dataTransformRecord)
                && Objects.equals(dataRemovalRecord, other.dataRemovalRecord)
                && Objects.equals(changeTargetExpr, other.changeTargetExpr) && Objects.equals(aliasVar, other.aliasVar)
                && Objects.equals(posVar, other.posVar) && Objects.equals(changeSeq, other.changeSeq)
                && Objects.equals(condition, other.condition) && type == other.type
                && Objects.equals(posExpr, other.posExpr) && Objects.equals(sourceExpr, other.sourceExpr);
    }

}
