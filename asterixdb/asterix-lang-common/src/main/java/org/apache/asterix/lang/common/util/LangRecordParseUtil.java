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

import java.io.Serializable;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

@SuppressWarnings("unchecked")
public class LangRecordParseUtil {
    private static final String NOT_ALLOWED_EXPRESSIONS_ERROR_MESSAGE =
            "JSON record can only have expressions [%1$s, %2$s, %3$s]";
    private static final ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADOUBLE);
    private static final ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    private static final ISerializerDeserializer<AInt64> intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    private static final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    private static final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    private LangRecordParseUtil() {
    }

    private static void parseExpression(Expression expr, ArrayBackedValueStorage serialized)
            throws HyracksDataException {
        switch (expr.getKind()) {
            case LITERAL_EXPRESSION:
                parseLiteral((LiteralExpr) expr, serialized);
                break;
            case RECORD_CONSTRUCTOR_EXPRESSION:
                parseRecord((RecordConstructor) expr, serialized, true);
                break;
            case LIST_CONSTRUCTOR_EXPRESSION:
                parseList((ListConstructor) expr, serialized);
                break;
            default:
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_PARSE_ERROR,
                        NOT_ALLOWED_EXPRESSIONS_ERROR_MESSAGE,
                        new Serializable[] { Expression.Kind.LITERAL_EXPRESSION.toString(),
                                Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION.toString(),
                                Expression.Kind.LIST_CONSTRUCTOR_EXPRESSION.toString() });
        }
    }

    public static void parseRecord(RecordConstructor recordValue, ArrayBackedValueStorage serialized, boolean tagged)
            throws HyracksDataException {
        AMutableString fieldNameString = new AMutableString(null);
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        RecordBuilder recordBuilder = new RecordBuilder();
        recordBuilder.reset(ARecordType.FULLY_OPEN_RECORD_TYPE);
        recordBuilder.init();
        List<FieldBinding> fbList = recordValue.getFbList();
        for (FieldBinding fb : fbList) {
            fieldName.reset();
            fieldValue.reset();
            // get key
            Expression keyExpr = fb.getLeftExpr();
            if (keyExpr.getKind() != Expression.Kind.LITERAL_EXPRESSION) {
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_PARSE_ERROR,
                        "JSON key can only be of type %1$s", Expression.Kind.LITERAL_EXPRESSION);
            }
            LiteralExpr keyLiteralExpr = (LiteralExpr) keyExpr;
            Literal keyLiteral = keyLiteralExpr.getValue();
            if (keyLiteral.getLiteralType() != Literal.Type.STRING) {
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_PARSE_ERROR,
                        "JSON key can only be of type %1$s", Literal.Type.STRING);
            }
            fieldNameString.setValue(keyLiteral.getStringValue());
            stringSerde.serialize(fieldNameString, fieldName.getDataOutput());
            // get value
            parseExpression(fb.getRightExpr(), fieldValue);
            recordBuilder.addField(fieldName, fieldValue);
        }
        recordBuilder.write(serialized.getDataOutput(), tagged);
    }

    private static void parseList(ListConstructor valueExpr, ArrayBackedValueStorage serialized)
            throws HyracksDataException {
        if (valueExpr.getType() != ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR) {
            throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_PARSE_ERROR,
                    "JSON List can't be of type %1$s", valueExpr.getType());
        }
        ArrayBackedValueStorage serializedValue = new ArrayBackedValueStorage();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(null);
        for (Expression expr : valueExpr.getExprList()) {
            serializedValue.reset();
            parseExpression(expr, serializedValue);
            listBuilder.addItem(serializedValue);
        }
        listBuilder.write(serialized.getDataOutput(), true);
    }

    private static void parseLiteral(LiteralExpr objectExpr, ArrayBackedValueStorage serialized)
            throws HyracksDataException {
        Literal value = objectExpr.getValue();
        switch (value.getLiteralType()) {
            case DOUBLE:
                doubleSerde.serialize(new ADouble((Double) value.getValue()), serialized.getDataOutput());
                break;
            case TRUE:
                booleanSerde.serialize(ABoolean.TRUE, serialized.getDataOutput());
                break;
            case FALSE:
                booleanSerde.serialize(ABoolean.FALSE, serialized.getDataOutput());
                break;
            case FLOAT:
                doubleSerde.serialize(new ADouble((Float) value.getValue()), serialized.getDataOutput());
                break;
            case INTEGER:
                intSerde.serialize(new AInt64(((Integer) value.getValue()).longValue()), serialized.getDataOutput());
                break;
            case LONG:
                intSerde.serialize(new AInt64((Long) value.getValue()), serialized.getDataOutput());
                break;
            case NULL:
                nullSerde.serialize(ANull.NULL, serialized.getDataOutput());
                break;
            case STRING:
                stringSerde.serialize(new AString((String) value.getValue()), serialized.getDataOutput());
                break;
            default:
                throw new HyracksDataException(ErrorCode.ASTERIX, ErrorCode.ERROR_PARSE_ERROR,
                        "Unknown Literal Type %1$s", value.getLiteralType());
        }
    }
}
