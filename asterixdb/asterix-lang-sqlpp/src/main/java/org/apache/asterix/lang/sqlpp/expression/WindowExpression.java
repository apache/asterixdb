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
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class WindowExpression extends AbstractExpression {

    private FunctionSignature functionSignature;
    private List<Expression> exprList;

    private List<Expression> partitionList;
    private List<Expression> orderbyList;
    private List<OrderbyClause.OrderModifier> orderbyModifierList;

    private FrameMode frameMode;
    private FrameBoundaryKind frameStartKind;
    private Expression frameStartExpr;
    private FrameBoundaryKind frameEndKind;
    private Expression frameEndExpr;
    private FrameExclusionKind frameExclusionKind;

    private VariableExpr windowVar;
    private List<Pair<Expression, Identifier>> windowFieldList;

    private Boolean ignoreNulls;
    private Boolean fromLast;

    public WindowExpression(FunctionSignature functionSignature, List<Expression> exprList,
            List<Expression> partitionList, List<Expression> orderbyList,
            List<OrderbyClause.OrderModifier> orderbyModifierList, FrameMode frameMode,
            FrameBoundaryKind frameStartKind, Expression frameStartExpr, FrameBoundaryKind frameEndKind,
            Expression frameEndExpr, FrameExclusionKind frameExclusionKind, VariableExpr windowVar,
            List<Pair<Expression, Identifier>> windowFieldList, Boolean ignoreNulls, Boolean fromLast) {
        if (functionSignature == null || exprList == null) {
            throw new NullPointerException();
        }
        this.functionSignature = functionSignature;
        this.exprList = exprList;
        this.partitionList = partitionList;
        this.orderbyList = orderbyList;
        this.orderbyModifierList = orderbyModifierList;
        this.frameMode = frameMode;
        this.frameStartKind = frameStartKind;
        this.frameStartExpr = frameStartExpr;
        this.frameEndKind = frameEndKind;
        this.frameEndExpr = frameEndExpr;
        this.frameExclusionKind = frameExclusionKind;
        this.windowVar = windowVar;
        this.windowFieldList = windowFieldList;
        this.ignoreNulls = ignoreNulls;
        this.fromLast = fromLast;
    }

    @Override
    public Kind getKind() {
        return Kind.WINDOW_EXPRESSION;
    }

    public FunctionSignature getFunctionSignature() {
        return functionSignature;
    }

    public void setFunctionSignature(FunctionSignature functionSignature) {
        if (functionSignature == null) {
            throw new NullPointerException();
        }
        this.functionSignature = functionSignature;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public void setExprList(List<Expression> exprList) {
        if (exprList == null) {
            throw new NullPointerException();
        }
        this.exprList = exprList;
    }

    public boolean hasPartitionList() {
        return partitionList != null && !partitionList.isEmpty();
    }

    public List<Expression> getPartitionList() {
        return partitionList;
    }

    public void setPartitionList(List<Expression> partitionList) {
        this.partitionList = partitionList;
    }

    public boolean hasOrderByList() {
        return orderbyList != null && !orderbyList.isEmpty();
    }

    public List<Expression> getOrderbyList() {
        return orderbyList;
    }

    public void setOrderbyList(List<Expression> orderbyList) {
        this.orderbyList = orderbyList;
    }

    public List<OrderbyClause.OrderModifier> getOrderbyModifierList() {
        return orderbyModifierList;
    }

    public void setOrderbyModifierList(List<OrderbyClause.OrderModifier> orderbyModifierList) {
        this.orderbyModifierList = orderbyModifierList;
    }

    public boolean hasFrameDefinition() {
        return frameMode != null;
    }

    public FrameMode getFrameMode() {
        return frameMode;
    }

    public void setFrameMode(FrameMode frameMode) {
        this.frameMode = frameMode;
    }

    public FrameBoundaryKind getFrameStartKind() {
        return frameStartKind;
    }

    public void setFrameStartKind(FrameBoundaryKind frameStartKind) {
        this.frameStartKind = frameStartKind;
    }

    public boolean hasFrameStartExpr() {
        return frameStartExpr != null;
    }

    public Expression getFrameStartExpr() {
        return frameStartExpr;
    }

    public void setFrameStartExpr(Expression frameStartExpr) {
        this.frameStartExpr = frameStartExpr;
    }

    public FrameBoundaryKind getFrameEndKind() {
        return frameEndKind;
    }

    public void setFrameEndKind(FrameBoundaryKind frameEndKind) {
        this.frameEndKind = frameEndKind;
    }

    public boolean hasFrameEndExpr() {
        return frameEndExpr != null;
    }

    public Expression getFrameEndExpr() {
        return frameEndExpr;
    }

    public void setFrameEndExpr(Expression frameEndExpr) {
        this.frameEndExpr = frameEndExpr;
    }

    public FrameExclusionKind getFrameExclusionKind() {
        return frameExclusionKind;
    }

    public void setFrameExclusionKind(FrameExclusionKind frameExclusionKind) {
        this.frameExclusionKind = frameExclusionKind;
    }

    public boolean hasWindowVar() {
        return windowVar != null;
    }

    public VariableExpr getWindowVar() {
        return windowVar;
    }

    public void setWindowVar(VariableExpr windowVar) {
        this.windowVar = windowVar;
    }

    public boolean hasWindowFieldList() {
        return windowFieldList != null && !windowFieldList.isEmpty();
    }

    public List<Pair<Expression, Identifier>> getWindowFieldList() {
        return windowFieldList;
    }

    public void setWindowFieldList(List<Pair<Expression, Identifier>> windowFieldList) {
        this.windowFieldList = windowFieldList;
    }

    public Boolean getIgnoreNulls() {
        return ignoreNulls;
    }

    public void setIgnoreNulls(Boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    public Boolean getFromLast() {
        return fromLast;
    }

    public void setFromLast(Boolean fromLast) {
        this.fromLast = fromLast;
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionSignature, exprList, ExpressionUtils.emptyIfNull(partitionList),
                ExpressionUtils.emptyIfNull(orderbyList), ExpressionUtils.emptyIfNull(orderbyModifierList), frameMode,
                frameStartKind, frameStartExpr, frameEndKind, frameEndExpr, frameExclusionKind, windowVar,
                ExpressionUtils.emptyIfNull(windowFieldList), ignoreNulls, fromLast);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof WindowExpression)) {
            return false;
        }
        WindowExpression target = (WindowExpression) object;
        return Objects.equals(functionSignature, target.functionSignature) && Objects.equals(exprList, target.exprList)
                && Objects.equals(ExpressionUtils.emptyIfNull(partitionList),
                        ExpressionUtils.emptyIfNull(target.partitionList))
                && Objects.equals(ExpressionUtils.emptyIfNull(orderbyList),
                        ExpressionUtils.emptyIfNull(target.orderbyList))
                && Objects.equals(ExpressionUtils.emptyIfNull(orderbyModifierList),
                        ExpressionUtils.emptyIfNull(target.orderbyModifierList))
                && frameMode == target.frameMode && frameStartKind == target.frameStartKind
                && Objects.equals(frameStartExpr, target.frameStartExpr) && frameEndKind == target.frameEndKind
                && Objects.equals(frameEndExpr, target.frameEndExpr) && frameExclusionKind == target.frameExclusionKind
                && Objects.equals(windowVar, target.windowVar)
                && Objects.equals(ExpressionUtils.emptyIfNull(windowFieldList),
                        ExpressionUtils.emptyIfNull(target.windowFieldList))
                && Objects.equals(ignoreNulls, target.ignoreNulls) && Objects.equals(fromLast, target.fromLast);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("WINDOW ");
        sb.append(functionSignature);
        sb.append('(');
        sb.append(StringUtils.join(exprList, ','));
        sb.append(')');
        if (fromLast != null && fromLast) {
            sb.append(" FROM LAST");
        }
        if (ignoreNulls != null && ignoreNulls) {
            sb.append(" IGNORE NULLS");
        }
        sb.append(" OVER ");
        if (hasWindowVar()) {
            sb.append(windowVar);
            if (hasWindowFieldList()) {
                sb.append('{');
                for (int i = 0, ln = windowFieldList.size(); i < ln; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    Pair<Expression, Identifier> p = windowFieldList.get(i);
                    sb.append(p.first).append(':').append(p.second);
                }
                sb.append('}');
            }
            sb.append(" AS ");
        }
        sb.append('(');
        if (hasPartitionList()) {
            sb.append(" PARTITION BY ");
            sb.append(StringUtils.join(partitionList, ','));
        }
        if (hasOrderByList()) {
            sb.append(" ORDER BY ");
            for (int i = 0, ln = orderbyList.size(); i < ln; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(orderbyList.get(i)).append(' ').append(orderbyModifierList.get(i));
            }
        }
        if (hasFrameDefinition()) {
            sb.append(" FRAME ").append(frameMode);
            sb.append(" BETWEEN ").append(frameStartKind);
            if (hasFrameStartExpr()) {
                sb.append(' ').append(frameStartExpr);
            }
            sb.append(" AND ").append(frameEndKind);
            if (hasFrameEndExpr()) {
                sb.append(' ').append(frameEndExpr);
            }
            sb.append(" EXCLUDE ").append(frameExclusionKind);
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    public enum FrameMode {
        RANGE("range"),
        ROWS("rows"),
        GROUPS("groups");

        private String text;

        FrameMode(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public enum FrameBoundaryKind {
        CURRENT_ROW("current row"),
        UNBOUNDED_PRECEDING("unbounded preceding"),
        UNBOUNDED_FOLLOWING("unbounded following"),
        BOUNDED_PRECEDING("preceding"),
        BOUNDED_FOLLOWING("following");

        private String text;

        FrameBoundaryKind(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public enum FrameExclusionKind {
        CURRENT_ROW("current row"),
        GROUP("group"),
        TIES("ties"),
        NO_OTHERS("no others");

        private String text;

        FrameExclusionKind(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }
}
