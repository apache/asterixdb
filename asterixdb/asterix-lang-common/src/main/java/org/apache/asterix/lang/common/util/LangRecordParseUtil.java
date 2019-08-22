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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmNullNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

// TODO(ali): all the functionality here is the same as the ones in ExpressionUtils
public class LangRecordParseUtil {
    private static final String NOT_ALLOWED_EXPRESSIONS_ERROR_MESSAGE =
            "JSON record can only have expressions [%1$s, %2$s, %3$s]";

    private LangRecordParseUtil() {
    }

    private static IAdmNode parseExpression(Expression expr) throws HyracksDataException, CompilationException {
        switch (expr.getKind()) {
            case LITERAL_EXPRESSION:
                return parseLiteral((LiteralExpr) expr);
            case RECORD_CONSTRUCTOR_EXPRESSION:
                return parseRecord((RecordConstructor) expr, Collections.emptyList());
            case LIST_CONSTRUCTOR_EXPRESSION:
                return parseList((ListConstructor) expr);
            default:
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.PARSE_ERROR,
                        NOT_ALLOWED_EXPRESSIONS_ERROR_MESSAGE, Expression.Kind.LITERAL_EXPRESSION.toString(),
                        Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION.toString(),
                        Expression.Kind.LIST_CONSTRUCTOR_EXPRESSION.toString());
        }
    }

    public static AdmObjectNode parseRecord(RecordConstructor recordValue, List<Pair<String, String>> defaults)
            throws HyracksDataException, CompilationException {
        AdmObjectNode record = new AdmObjectNode();
        List<FieldBinding> fbList = recordValue.getFbList();
        for (FieldBinding fb : fbList) {
            // get key
            String key = exprToStringLiteral(fb.getLeftExpr()).getStringValue();
            // get value
            IAdmNode value = parseExpression(fb.getRightExpr());
            record.set(key, value);
        }
        // defaults
        for (Pair<String, String> kv : defaults) {
            record.set(kv.first, new AdmStringNode(kv.second));
        }
        return record;
    }

    public static Literal exprToStringLiteral(Expression expr) throws CompilationException {
        if (expr.getKind() != Expression.Kind.LITERAL_EXPRESSION) {
            throw new CompilationException(ErrorCode.PARSE_ERROR, "Expected expression can only be of type %1$s",
                    Expression.Kind.LITERAL_EXPRESSION);
        }
        LiteralExpr keyLiteralExpr = (LiteralExpr) expr;
        Literal keyLiteral = keyLiteralExpr.getValue();
        if (keyLiteral.getLiteralType() != Literal.Type.STRING) {
            throw new CompilationException(ErrorCode.PARSE_ERROR, "Expected Literal can only be of type %1$s",
                    Literal.Type.STRING);
        }
        return keyLiteral;
    }

    private static AdmArrayNode parseList(ListConstructor valueExpr) throws CompilationException, HyracksDataException {
        if (valueExpr.getType() != ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR) {
            throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.PARSE_ERROR, "JSON List can't be of type %1$s",
                    valueExpr.getType());
        }
        List<Expression> exprs = valueExpr.getExprList();
        AdmArrayNode list = new AdmArrayNode(exprs.size());
        for (Expression expr : exprs) {
            list.add(parseExpression(expr));
        }
        return list;
    }

    private static IAdmNode parseLiteral(LiteralExpr objectExpr) throws HyracksDataException {
        Literal value = objectExpr.getValue();
        switch (value.getLiteralType()) {
            case DOUBLE:
            case FLOAT:
                return new AdmDoubleNode((Double) value.getValue());
            case TRUE:
                return AdmBooleanNode.TRUE;
            case FALSE:
                return AdmBooleanNode.FALSE;
            case INTEGER:
                return new AdmBigIntNode((Integer) value.getValue());
            case LONG:
                return new AdmBigIntNode((Long) value.getValue());
            case NULL:
                return AdmNullNode.INSTANCE;
            case STRING:
                return new AdmStringNode((String) value.getValue());
            default:
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.PARSE_ERROR, "Unknown Literal Type %1$s",
                        value.getLiteralType());
        }
    }

    public static void recordToMap(Map<String, String> map, ARecord record) throws AlgebricksException {
        String[] keys = record.getType().getFieldNames();
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            String value = aObjToString(record.getValueByPos(i));
            map.put(key, value);
        }
    }

    public static void recordToMap(Map<String, String> map, AdmObjectNode record) throws AlgebricksException {
        for (Entry<String, IAdmNode> field : record.getFields()) {
            String value = aObjToString(field.getValue());
            map.put(field.getKey(), value);
        }
    }

    private static String aObjToString(IAdmNode aObj) {
        if (aObj.getType() == ATypeTag.STRING) {
            return ((AdmStringNode) aObj).get();
        } else {
            return aObj.toString();
        }
    }

    private static String aObjToString(IAObject aObj) throws AlgebricksException {
        switch (aObj.getType().getTypeTag()) {
            case DOUBLE:
                return Double.toString(((ADouble) aObj).getDoubleValue());
            case BIGINT:
                return Long.toString(((AInt64) aObj).getLongValue());
            case ARRAY:
                return aOrderedListToString((AOrderedList) aObj);
            case STRING:
                return ((AString) aObj).getStringValue();
            case BOOLEAN:
                return ((ABoolean) aObj).getBoolean().toString();
            default:
                throw new AlgebricksException("value of type " + aObj.getType() + " is not supported yet");
        }
    }

    private static String aOrderedListToString(AOrderedList ol) throws AlgebricksException {
        StringBuilder delimitedList = new StringBuilder();
        IACursor cursor = ol.getCursor();
        if (cursor.next()) {
            IAObject next = cursor.get();
            delimitedList.append(aObjToString(next));
        }
        while (cursor.next()) {
            IAObject next = cursor.get();
            delimitedList.append(",");
            delimitedList.append(aObjToString(next));
        }
        return delimitedList.toString();
    }
}
