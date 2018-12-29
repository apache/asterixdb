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
package org.apache.asterix.lang.common.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.DoubleLiteral;
import org.apache.asterix.lang.common.literal.LongIntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmNullNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;

public class ExpressionUtils {
    private ExpressionUtils() {
    }

    public static IAdmNode toNode(Expression expr) throws CompilationException {
        switch (expr.getKind()) {
            case LIST_CONSTRUCTOR_EXPRESSION:
                return toNode((ListConstructor) expr);
            case LITERAL_EXPRESSION:
                return toNode((LiteralExpr) expr);
            case RECORD_CONSTRUCTOR_EXPRESSION:
                return toNode((RecordConstructor) expr);
            default:
                throw new CompilationException(ErrorCode.EXPRESSION_NOT_SUPPORTED_IN_CONSTANT_RECORD, expr.getKind());
        }
    }

    public static AdmObjectNode toNode(RecordConstructor recordConstructor) throws CompilationException {
        AdmObjectNode node = new AdmObjectNode();
        final List<FieldBinding> fbList = recordConstructor.getFbList();
        for (int i = 0; i < fbList.size(); i++) {
            FieldBinding binding = fbList.get(i);
            String key = LangRecordParseUtil.exprToStringLiteral(binding.getLeftExpr()).getStringValue();
            IAdmNode value = ExpressionUtils.toNode(binding.getRightExpr());
            node.set(key, value);
        }
        return node;
    }

    private static IAdmNode toNode(ListConstructor listConstructor) throws CompilationException {
        final List<Expression> exprList = listConstructor.getExprList();
        AdmArrayNode array = new AdmArrayNode(exprList.size());
        for (int i = 0; i < exprList.size(); i++) {
            array.add(ExpressionUtils.toNode(exprList.get(i)));
        }
        return array;
    }

    private static IAdmNode toNode(LiteralExpr literalExpr) throws CompilationException {
        final Literal value = literalExpr.getValue();
        final Literal.Type literalType = value.getLiteralType();
        switch (literalType) {
            case DOUBLE:
                return new AdmDoubleNode(((DoubleLiteral) value).getDoubleValue());
            case FALSE:
            case TRUE:
                return AdmBooleanNode.get((Boolean) value.getValue());
            case LONG:
                return new AdmBigIntNode(((LongIntegerLiteral) value).getLongValue());
            case NULL:
                return AdmNullNode.INSTANCE;
            case STRING:
                return new AdmStringNode(((StringLiteral) value).getValue());
            default:
                throw new CompilationException(ErrorCode.LITERAL_TYPE_NOT_SUPPORTED_IN_CONSTANT_RECORD, literalType);
        }
    }

    public static <T> Collection<T> emptyIfNull(Collection<T> coll) {
        return coll == null ? Collections.emptyList() : coll;
    }
}
