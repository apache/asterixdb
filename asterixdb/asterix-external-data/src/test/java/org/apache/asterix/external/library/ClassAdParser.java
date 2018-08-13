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
package org.apache.asterix.external.library;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.classad.AMutableCharArrayString;
import org.apache.asterix.external.classad.AMutableNumberFactor;
import org.apache.asterix.external.classad.AttributeReference;
import org.apache.asterix.external.classad.CaseInsensitiveString;
import org.apache.asterix.external.classad.CharArrayLexerSource;
import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.ExprList;
import org.apache.asterix.external.classad.ExprTree;
import org.apache.asterix.external.classad.ExprTree.NodeKind;
import org.apache.asterix.external.classad.ExprTreeHolder;
import org.apache.asterix.external.classad.FileLexerSource;
import org.apache.asterix.external.classad.FunctionCall;
import org.apache.asterix.external.classad.InputStreamLexerSource;
import org.apache.asterix.external.classad.Lexer;
import org.apache.asterix.external.classad.Lexer.TokenType;
import org.apache.asterix.external.classad.LexerSource;
import org.apache.asterix.external.classad.Literal;
import org.apache.asterix.external.classad.Operation;
import org.apache.asterix.external.classad.StringLexerSource;
import org.apache.asterix.external.classad.TokenValue;
import org.apache.asterix.external.classad.Value;
import org.apache.asterix.external.classad.Value.NumberFactor;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.parser.AbstractDataParser;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/// This reads ClassAd strings from various sources and converts them into a ClassAd.
/// It can read from Strings, Files, and InputStreams.
public class ClassAdParser extends AbstractDataParser implements IRecordDataParser<char[]> {

    // reusable components
    private Lexer lexer = new Lexer();
    private LexerSource currentSource = null;
    private boolean isExpr = false;
    private final ClassAdObjectPool objectPool;
    // asterix objects
    private ARecordType recordType;
    private IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool =
            new ListObjectPool<IARecordBuilder, ATypeTag>(new RecordBuilderFactory());
    private IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool =
            new ListObjectPool<IAsterixListBuilder, ATypeTag>(new ListBuilderFactory());
    private IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool =
            new ListObjectPool<IMutableValueStorage, ATypeTag>(new AbvsBuilderFactory());
    private final ClassAd rootAd;
    private String exprPrefix = "expr=";
    private String exprSuffix = "";
    private boolean evaluateExpr = true;
    private String exprFieldNameSuffix = "Expr";
    private boolean keepBoth = true;
    private boolean oldFormat = true;
    private StringLexerSource stringLexerSource = new StringLexerSource("");

    public ClassAdParser(ARecordType recordType, boolean oldFormat, boolean evaluateExpr, boolean keepBoth,
            String exprPrefix, String exprSuffix, String exprFieldNameSuffix, ClassAdObjectPool objectPool) {
        if (objectPool == null) {
            System.out.println();
        }
        this.objectPool = objectPool;
        this.rootAd = new ClassAd(objectPool);
        this.recordType = recordType;
        this.currentSource = new CharArrayLexerSource();
        this.recordType = recordType;
        this.oldFormat = oldFormat;
        if (oldFormat) {
            rootAd.createParser();
        }
        this.keepBoth = keepBoth;
        this.evaluateExpr = evaluateExpr;
        this.exprPrefix = exprPrefix;
        this.exprSuffix = exprSuffix;
        this.exprFieldNameSuffix = exprFieldNameSuffix;
    }

    public ClassAdParser(ClassAdObjectPool objectPool) {
        if (objectPool == null) {
            System.out.println();
        }
        this.objectPool = objectPool;
        this.currentSource = new CharArrayLexerSource();
        rootAd = null;
    }

    /***********************************
     * AsterixDB Specific begin
     *
     * @throws AsterixException
     ***********************************/
    public void asterixParse(ClassAd classad, DataOutput out) throws IOException, AsterixException {
        // we assume the lexer source used here is a char array
        parseClassAd(currentSource, classad, false);
        parseRecord(null, classad, out);
    }

    public void handleErrorParsing() throws IOException {
    }

    private boolean asterixParseClassAd(ClassAd ad) throws IOException {
        TokenType tt;
        ad.clear();
        lexer.initialize(currentSource);
        if ((tt = lexer.consumeToken()) != TokenType.LEX_OPEN_BOX) {
            handleErrorParsing();
            return false;
        }
        tt = lexer.peekToken();
        TokenValue tv = objectPool.tokenValuePool.get();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        while (tt != TokenType.LEX_CLOSE_BOX) {
            // Get the name of the expression
            tv.reset();
            tree.reset();
            tt = lexer.consumeToken(tv);
            if (tt == TokenType.LEX_SEMICOLON) {
                // We allow empty expressions, so if someone give a double
                // semicolon, it doesn't
                // hurt. Technically it's not right, but we shouldn't make users
                // pay the price for
                // a meaningless mistake. See condor-support #1881 for a user
                // that was bitten by this.
                continue;
            }
            if (tt != TokenType.LEX_IDENTIFIER) {
                throw new HyracksDataException(
                        "while parsing classad:  expected LEX_IDENTIFIER " + " but got " + Lexer.strLexToken(tt));
            }

            // consume the intermediate '='
            if ((tt = lexer.consumeToken()) != TokenType.LEX_BOUND_TO) {
                throw new HyracksDataException(
                        "while parsing classad:  expected LEX_BOUND_TO " + " but got " + Lexer.strLexToken(tt));
            }

            int positionBefore = lexer.getLexSource().getPosition();
            isExpr = false;
            // parse the expression
            parseExpression(tree);
            if (tree.getInnerTree() == null) {
                handleErrorParsing();
                throw new HyracksDataException("parse expression returned empty tree");
            }

            if ((!evaluateExpr || keepBoth) && isExpr && positionBefore >= 0) {
                // we will store a string representation of the expression
                int len = lexer.getLexSource().getPosition() - positionBefore - 2;
                // add it as it is to the classAd
                Literal lit = objectPool.literalPool.get();
                Value exprVal = objectPool.valuePool.get();

                exprVal.setStringValue((exprPrefix == null ? "" : exprPrefix)
                        + String.valueOf(lexer.getLexSource().getBuffer(), positionBefore, len)
                        + (exprSuffix == null ? "" : exprSuffix));
                Literal.createLiteral(lit, exprVal, NumberFactor.NO_FACTOR);
                if (!evaluateExpr) {
                    ad.insert(tv.getStrValue().toString(), lit);
                } else {
                    ad.insert(tv.getStrValue().toString() + exprFieldNameSuffix, lit);
                }
            }
            if (!isExpr || (evaluateExpr)) {
                // insert the attribute into the classad
                if (!ad.insert(tv.getStrValue().toString(), tree)) {
                    handleErrorParsing();
                    throw new HyracksDataException("Couldn't insert value to classad");
                }
            }
            // the next token must be a ';' or a ']'
            tt = lexer.peekToken();
            if (tt != TokenType.LEX_SEMICOLON && tt != TokenType.LEX_CLOSE_BOX) {
                handleErrorParsing();
                throw new HyracksDataException("while parsing classad:  expected LEX_SEMICOLON or "
                        + "LEX_CLOSE_BOX but got " + Lexer.strLexToken(tt));
            }

            // Slurp up any extra semicolons. This does not duplicate the work
            // at the top of the loop
            // because it accounts for the case where the last expression has
            // extra semicolons,
            // while the first case accounts for optional beginning semicolons.
            while (tt == TokenType.LEX_SEMICOLON) {
                lexer.consumeToken();
                tt = lexer.peekToken();
            }
        }
        return true;
    }

    public static String readLine(char[] buffer, AMutableInt32 offset, int maxOffset) {
        int position = offset.getIntegerValue();
        while (buffer[position] != '\n' && position < maxOffset) {
            position++;
        }
        if (offset.getIntegerValue() == position) {
            return null;
        }
        String line = String.valueOf(buffer, offset.getIntegerValue(), position - offset.getIntegerValue());
        position++;
        offset.setValue(position);
        return line;
    }

    private AMutableInt32 aInt32 = new AMutableInt32(0);

    /**
     * Resets the pools before parsing a top-level record. In this way the
     * elements in those pools can be re-used.
     */
    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
        objectPool.reset();
    }

    private ATypeTag getTargetTypeTag(ATypeTag expectedTypeTag, IAType aObjectType) throws IOException {
        if (aObjectType == null) {
            return expectedTypeTag;
        }
        if (aObjectType.getTypeTag() != ATypeTag.UNION) {
            final ATypeTag typeTag = aObjectType.getTypeTag();
            if (ATypeHierarchy.canPromote(expectedTypeTag, typeTag)
                    || ATypeHierarchy.canDemote(expectedTypeTag, typeTag)) {
                return typeTag;
            } else {
                return null;
            }
        } else { // union
            List<IAType> unionList = ((AUnionType) aObjectType).getUnionList();
            for (IAType t : unionList) {
                final ATypeTag typeTag = t.getTypeTag();
                if (ATypeHierarchy.canPromote(expectedTypeTag, typeTag)
                        || ATypeHierarchy.canDemote(expectedTypeTag, typeTag)) {
                    return typeTag;
                }
            }
        }
        return null;
    }

    private void parseRecord(ARecordType recType, ClassAd pAd, DataOutput out) throws IOException, AsterixException {
        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();
        BitSet nulls = null;
        if (recType != null) {
            nulls = getBitSet();
            recBuilder.reset(recType);
        } else {
            recBuilder.reset(null);
        }
        recBuilder.init();
        Boolean openRecordField = false;
        int fieldId = 0;
        IAType fieldType = null;

        // new stuff
        Map<CaseInsensitiveString, ExprTree> attrs = pAd.getAttrList();
        for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
            // reset buffers
            fieldNameBuffer.reset();
            fieldValueBuffer.reset();
            // take care of field name
            String fldName = entry.getKey().get();
            if (recType != null) {
                fieldId = recBuilder.getFieldId(fldName);
                if (fieldId < 0 && !recType.isOpen()) {
                    throw new HyracksDataException("This record is closed, you can not add extra fields !!");
                } else if (fieldId < 0 && recType.isOpen()) {
                    aStringFieldName.setValue(fldName);
                    stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                    openRecordField = true;
                    fieldType = null;
                } else {
                    // a closed field
                    nulls.set(fieldId);
                    fieldType = recType.getFieldTypes()[fieldId];
                    openRecordField = false;
                }
            } else {
                aStringFieldName.setValue(fldName);
                stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                openRecordField = true;
                fieldType = null;
            }

            // add field value to value buffer
            writeFieldValueToBuffer(fieldType, fieldValueBuffer.getDataOutput(), fldName, entry.getValue(), pAd);
            if (openRecordField) {
                if (fieldValueBuffer.getByteArray()[0] != ATypeTag.MISSING.serialize()) {
                    recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                }
            } else if (NonTaggedFormatUtil.isOptional(fieldType)) {
                if (fieldValueBuffer.getByteArray()[0] != ATypeTag.MISSING.serialize()) {
                    recBuilder.addField(fieldId, fieldValueBuffer);
                }
            } else {
                recBuilder.addField(fieldId, fieldValueBuffer);
            }
        }

        if (recType != null) {
            int optionalFieldId = checkOptionalConstraints(recType, nulls);
            if (optionalFieldId != -1) {
                throw new HyracksDataException(
                        "Field: " + recType.getFieldNames()[optionalFieldId] + " can not be optional");
            }
        }
        recBuilder.write(out, true);
    }

    private void writeFieldValueToBuffer(IAType fieldType, DataOutput out, String name, ExprTree tree, ClassAd pAd)
            throws IOException, AsterixException {
        Value val;
        switch (tree.getKind()) {
            case ATTRREF_NODE:
            case CLASSAD_NODE:
            case EXPR_ENVELOPE:
            case EXPR_LIST_NODE:
            case FN_CALL_NODE:
            case OP_NODE:
                val = objectPool.valuePool.get();
                if (pAd.evaluateAttr(name, val)) {
                } else {
                    // just write the expr
                    val = ((Literal) pAd.getAttrList().get(name + "Expr")).getValue();
                }
                break;
            case LITERAL_NODE:
                val = ((Literal) tree.getTree()).getValue();
                break;
            default:
                throw new HyracksDataException("Unknown Expression type detected: " + tree.getKind());
        }

        if (fieldType != null) {
            if (NonTaggedFormatUtil.isOptional(fieldType)) {
                fieldType = ((AUnionType) fieldType).getActualType();
            }
        }
        switch (val.getValueType()) {
            case ABSOLUTE_TIME_VALUE:
                if (checkType(ATypeTag.DATETIME, fieldType)) {
                    parseDateTime(val, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case BOOLEAN_VALUE:
                if (checkType(ATypeTag.BOOLEAN, fieldType)) {
                    booleanSerde.serialize(val.getBoolVal() ? ABoolean.TRUE : ABoolean.FALSE, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case CLASSAD_VALUE:
                if (checkType(ATypeTag.OBJECT, fieldType)) {
                    IAType objectType = getComplexType(fieldType, ATypeTag.OBJECT);
                    ClassAd classad = val.getClassadVal();
                    parseRecord((ARecordType) objectType, classad, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case ERROR_VALUE:
            case STRING_VALUE:
            case UNDEFINED_VALUE:
                if (checkType(ATypeTag.STRING, fieldType)) {
                    parseString(val, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case INTEGER_VALUE:
                if (checkType(ATypeTag.BIGINT, fieldType)) {
                    if (fieldType == null || fieldType.getTypeTag() == ATypeTag.BIGINT) {
                        aInt64.setValue(val.getLongVal());
                        int64Serde.serialize(aInt64, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.INTEGER) {
                        aInt32.setValue((int) val.getLongVal());
                        int32Serde.serialize(aInt32, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.DOUBLE) {
                        aDouble.setValue(val.getLongVal());
                        doubleSerde.serialize(aDouble, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.SMALLINT) {
                        aInt16.setValue((short) val.getLongVal());
                        int16Serde.serialize(aInt16, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.TINYINT) {
                        aInt8.setValue((byte) val.getLongVal());
                        int8Serde.serialize(aInt8, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.FLOAT) {
                        aFloat.setValue(val.getLongVal());
                        floatSerde.serialize(aFloat, out);
                    }
                } else if (checkType(ATypeTag.DATETIME, fieldType)) {
                    // Classad uses Linux Timestamps (s instead of ms)
                    aDateTime.setValue(val.getLongVal() * 1000);
                    datetimeSerde.serialize(aDateTime, out);
                } else if (checkType(ATypeTag.DURATION, fieldType)) {
                    // Classad uses Linux Timestamps (s instead of ms)
                    aDuration.setValue(0, val.getLongVal() * 1000);
                    durationSerde.serialize(aDuration, out);
                } else if (checkType(ATypeTag.INTEGER, fieldType)) {
                    aInt32.setValue((int) val.getLongVal());
                    int32Serde.serialize(aInt32, out);
                } else if (checkType(ATypeTag.DOUBLE, fieldType)) {
                    aDouble.setValue(val.getLongVal());
                    doubleSerde.serialize(aDouble, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case LIST_VALUE:
            case SLIST_VALUE:
                IAType objectType;
                if (checkType(ATypeTag.MULTISET, fieldType)) {
                    objectType = getComplexType(fieldType, ATypeTag.MULTISET);
                    parseUnorderedList((AUnorderedListType) objectType, val, out);
                } else if (checkType(ATypeTag.ARRAY, fieldType)) {
                    objectType = getComplexType(fieldType, ATypeTag.ARRAY);
                    parseOrderedList((AOrderedListType) objectType, val, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case REAL_VALUE:
                if (checkType(ATypeTag.DOUBLE, fieldType)) {
                    if (fieldType == null || fieldType.getTypeTag() == ATypeTag.DOUBLE) {
                        aDouble.setValue(val.getDoubleVal());
                        doubleSerde.serialize(aDouble, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.INTEGER) {
                        aInt32.setValue((int) val.getDoubleVal());
                        int32Serde.serialize(aInt32, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.BIGINT) {
                        aInt64.setValue((long) val.getDoubleVal());
                        int64Serde.serialize(aInt64, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.SMALLINT) {
                        aInt16.setValue((short) val.getDoubleVal());
                        int16Serde.serialize(aInt16, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.TINYINT) {
                        aInt8.setValue((byte) val.getDoubleVal());
                        int8Serde.serialize(aInt8, out);
                    } else if (fieldType.getTypeTag() == ATypeTag.FLOAT) {
                        aFloat.setValue((float) val.getDoubleVal());
                        floatSerde.serialize(aFloat, out);
                    }
                } else if (checkType(ATypeTag.INTEGER, fieldType)) {
                    aInt32.setValue((int) val.getDoubleVal());
                    int32Serde.serialize(aInt32, out);
                } else if (checkType(ATypeTag.BIGINT, fieldType)) {
                    aInt64.setValue((long) val.getDoubleVal());
                    int64Serde.serialize(aInt64, out);
                } else if (checkType(ATypeTag.DATETIME, fieldType)) {
                    // Classad uses Linux Timestamps (s instead of ms)
                    aDateTime.setValue(val.getLongVal() * 1000);
                    datetimeSerde.serialize(aDateTime, out);
                } else if (checkType(ATypeTag.DURATION, fieldType)) {
                    // Classad uses Linux Timestamps (s instead of ms)
                    aDuration.setValue(0, (long) (val.getDoubleVal() * 1000.0));
                    durationSerde.serialize(aDuration, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            case RELATIVE_TIME_VALUE:
                if (checkType(ATypeTag.DURATION, fieldType)) {
                    parseDuration(val, out);
                } else {
                    throw new HyracksDataException(mismatchErrorMessage + fieldType.getTypeTag());
                }
                break;
            default:
                throw new HyracksDataException("unknown data type " + val.getValueType());
        }
    }

    private void parseOrderedList(AOrderedListType oltype, Value listVal, DataOutput out)
            throws IOException, AsterixException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) getOrderedListBuilder();
        IAType itemType = null;
        if (oltype != null) {
            itemType = oltype.getItemType();
        }
        orderedListBuilder.reset(oltype);
        for (ExprTree tree : listVal.getListVal().getExprList()) {
            itemBuffer.reset();
            writeFieldValueToBuffer(itemType, itemBuffer.getDataOutput(), null, tree, null);
            orderedListBuilder.addItem(itemBuffer);
        }
        orderedListBuilder.write(out, true);
    }

    private void parseUnorderedList(AUnorderedListType uoltype, Value listVal, DataOutput out)
            throws IOException, AsterixException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();
        IAType itemType = null;
        if (uoltype != null) {
            itemType = uoltype.getItemType();
        }
        unorderedListBuilder.reset(uoltype);
        for (ExprTree tree : listVal.getListVal().getExprList()) {
            itemBuffer.reset();
            writeFieldValueToBuffer(itemType, itemBuffer.getDataOutput(), null, tree, null);
            unorderedListBuilder.addItem(itemBuffer);
        }
        unorderedListBuilder.write(out, true);
    }

    private void parseString(Value val, DataOutput out) throws HyracksDataException {
        switch (val.getValueType()) {
            case ERROR_VALUE:
                aString.setValue("error");
                break;
            case STRING_VALUE:
                aString.setValue(val.getStringVal());
                break;
            case UNDEFINED_VALUE:
                aString.setValue("undefined");
                break;
            default:
                throw new HyracksDataException("Unknown String type " + val.getValueType());
        }
        stringSerde.serialize(aString, out);
    }

    protected void parseDuration(Value duration, DataOutput out) throws HyracksDataException {
        try {
            aDuration.setValue(0, duration.getTimeVal().getRelativeTime());
            durationSerde.serialize(aDuration, out);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void parseDateTime(Value datetime, DataOutput out) throws HyracksDataException {
        aDateTime.setValue(datetime.getTimeVal().getTimeInMillis());
        datetimeSerde.serialize(aDateTime, out);
    }

    public static IAType getComplexType(IAType aObjectType, ATypeTag tag) {
        if (aObjectType == null) {
            return null;
        }

        if (aObjectType.getTypeTag() == tag) {
            return aObjectType;
        }

        if (aObjectType.getTypeTag() == ATypeTag.UNION) {
            List<IAType> unionList = ((AUnionType) aObjectType).getUnionList();
            for (int i = 0; i < unionList.size(); i++) {
                if (unionList.get(i).getTypeTag() == tag) {
                    return unionList.get(i);
                }
            }
        }
        return null; // wont get here
    }

    private String mismatchErrorMessage = "Mismatch Type, expecting a value of type ";

    private boolean checkType(ATypeTag expectedTypeTag, IAType aObjectType) throws IOException {
        return getTargetTypeTag(expectedTypeTag, aObjectType) != null;
    }

    private BitSet getBitSet() {
        return objectPool.bitSetPool.get();
    }

    public static int checkOptionalConstraints(ARecordType recType, BitSet nulls) {
        for (int i = 0; i < recType.getFieldTypes().length; i++) {
            if (nulls.get(i) == false) {
                IAType type = recType.getFieldTypes()[i];
                if (type.getTypeTag() != ATypeTag.MISSING && type.getTypeTag() != ATypeTag.UNION) {
                    return i;
                }

                if (type.getTypeTag() == ATypeTag.UNION) { // union
                    AUnionType unionType = (AUnionType) type;
                    if (!unionType.isUnknownableType()) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.OBJECT);
    }

    private IAsterixListBuilder getOrderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.ARRAY);
    }

    private IAsterixListBuilder getUnorderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.MULTISET);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    public static ATypeTag getMatchingType(Literal lit) throws HyracksDataException {
        return getMatchingType(lit.getValue());
    }

    public static ATypeTag getMatchingType(Value val) throws HyracksDataException {
        switch (val.getValueType()) {
            case ABSOLUTE_TIME_VALUE:
                return ATypeTag.DATETIME;
            case BOOLEAN_VALUE:
                return ATypeTag.BOOLEAN;
            case CLASSAD_VALUE:
                return ATypeTag.OBJECT;
            case ERROR_VALUE:
            case STRING_VALUE:
            case UNDEFINED_VALUE:
                return ATypeTag.STRING;
            case INTEGER_VALUE:
                return ATypeTag.BIGINT;
            case LIST_VALUE:
            case SLIST_VALUE:
                return ATypeTag.MULTISET;
            case NULL_VALUE:
                return ATypeTag.MISSING;
            case REAL_VALUE:
                return ATypeTag.DOUBLE;
            case RELATIVE_TIME_VALUE:
                return ATypeTag.DURATION;
            default:
                throw new HyracksDataException("Unknown data type");
        }
    }

    /********************************
     * End of AsterixDB specifics
     ********************************/

    /**
     * Parse a ClassAd
     *
     * @param buffer
     *            Buffer containing the string representation of the classad.
     * @param full
     *            If this parameter is true, the parse is considered to succeed
     *            only if the ClassAd was parsed successfully and no other
     *            tokens follow the ClassAd.
     * @return pointer to the ClassAd object if successful, or null otherwise
     * @throws IOException
     */
    public ClassAd parseClassAd(String buffer, boolean full) throws IOException {
        currentSource = new StringLexerSource(buffer);
        return parseClassAd(currentSource, full);
    }

    public ClassAd parseClassAd(String buffer, AMutableInt32 offset) throws IOException {
        currentSource = new StringLexerSource(buffer);
        ClassAd ad = parseClassAd((StringLexerSource) currentSource);
        offset.setValue(((StringLexerSource) currentSource).getCurrentLocation());
        return ad;
    }

    public ClassAd parseClassAd(StringLexerSource lexer_source) throws IOException {
        return parseClassAd(lexer_source, false);
    }

    public ClassAd parseClassAd(File file, boolean full) throws IOException {
        FileLexerSource fileLexerSource = new FileLexerSource(file);
        return parseClassAd(fileLexerSource, full);
    }

    public ClassAd parseClassAd(InputStream in, boolean full) throws IOException {
        InputStreamLexerSource lexer_source = new InputStreamLexerSource(in);
        return parseClassAd(lexer_source, full);
    }

    // preferred method since the parser doesn't need to create an object
    public void parseClassAd(ClassAd ad, LexerSource lexer_source, boolean full) throws IOException {
        ad.reset();
        if (lexer.initialize(lexer_source)) {
            if (!parseClassAd(ad, full)) {
                return;
            } else if (lexer_source.readPreviousCharacter() != '\0') {
                // The lexer swallows one extra character, so if we have
                // two classads back to back we need to make sure to unread
                // one of the characters.
                lexer_source.unreadCharacter();
            }
        }
    }

    public ClassAd parseClassAd(LexerSource lexer_source, boolean full) throws IOException {
        System.out.println("Don't use this call. instead, pass a mutable classad instance");
        ClassAd ad = objectPool.classAdPool.get();
        if (lexer.initialize(lexer_source)) {
            if (!parseClassAd(ad, full)) {
                return null;
            } else if (lexer_source.readPreviousCharacter() != '\0') {
                // The lexer swallows one extra character, so if we have
                // two classads back to back we need to make sure to unread
                // one of the characters.
                lexer_source.unreadCharacter();
            }
        }
        return ad;
    }

    /**
     * Parse a ClassAd
     *
     * @param buffer
     *            Buffer containing the string representation of the classad.
     * @param ad
     *            The classad to be populated
     * @param full
     *            If this parameter is true, the parse is considered to succeed
     *            only if the ClassAd was parsed successfully and no other
     *            tokens follow the ClassAd.
     * @return true on success, false on failure
     * @throws IOException
     */
    public boolean parseClassAd(String buffer, ClassAd classad, boolean full) throws IOException {
        StringLexerSource stringLexerSource = new StringLexerSource(buffer);
        return parseClassAd(stringLexerSource, classad, full);
    }

    public boolean parseClassAd(String buffer, ClassAd classad, AMutableInt32 offset) throws IOException {
        boolean success = false;
        StringLexerSource stringLexerSource = new StringLexerSource(buffer, offset.getIntegerValue());
        success = parseClassAd(stringLexerSource, classad);
        offset.setValue(stringLexerSource.getCurrentLocation());
        return success;
    }

    public boolean parseNext(ClassAd classad) throws IOException {
        resetPools();
        return parseClassAd(currentSource, classad, false);
    }

    public boolean parseNext(ClassAd classad, boolean full) throws IOException {
        return parseClassAd(currentSource, classad, full);
    }

    private boolean parseClassAd(StringLexerSource lexer_source, ClassAd classad) throws IOException {
        return parseClassAd(lexer_source, classad, false);
    }

    public boolean parseClassAd(File file, ClassAd classad, boolean full) throws IOException {
        FileLexerSource fileLexerSource = new FileLexerSource(file);
        return parseClassAd(fileLexerSource, classad, full);
    }

    public boolean parseClassAd(InputStream stream, ClassAd classad, boolean full) throws IOException {
        InputStreamLexerSource inputStreamLexerSource = new InputStreamLexerSource(stream);
        return parseClassAd(inputStreamLexerSource, classad, full);
    }

    public boolean parseClassAd(LexerSource lexer_source, ClassAd classad, boolean full) throws IOException {
        boolean success = false;
        if (lexer.initialize(lexer_source)) {
            success = parseClassAd(classad, full);
        }
        if (success) {
            // The lexer swallows one extra character, so if we have
            // two classads back to back we need to make sure to unread
            // one of the characters.
            if (lexer_source.readPreviousCharacter() != Lexer.EOF) {
                lexer_source.unreadCharacter();
            }
        } else {
            classad.clear();
        }
        return success;
    }

    /**
     * Parse an expression
     *
     * @param buffer
     *            Buffer containing the string representation of the expression.
     * @param full
     *            If this parameter is true, the parse is considered to succeed
     *            only if the expression was parsed successfully and no other
     *            tokens are left.
     * @return pointer to the expression object if successful, or null otherwise
     */
    public ExprTree parseExpression(String buffer, boolean full) throws IOException {
        stringLexerSource.setNewSource(buffer);
        ExprTreeHolder mutableExpr = objectPool.mutableExprPool.get();
        if (lexer.initialize(stringLexerSource)) {
            parseExpression(mutableExpr, full);
        }
        return mutableExpr.getInnerTree();
    }

    public ExprTree ParseExpression(String buffer) throws IOException {
        return parseExpression(buffer, false);
    }

    public ExprTree parseExpression(LexerSource lexer_source, boolean full) throws IOException {
        ExprTreeHolder mutableExpr = objectPool.mutableExprPool.get();
        if (lexer.initialize(lexer_source)) {
            parseExpression(mutableExpr, full);
        }
        return mutableExpr.getInnerTree();
    }

    public ExprTree parseNextExpression() throws IOException {
        if (!lexer.wasInitialized()) {
            return null;
        } else {
            ExprTreeHolder expr = objectPool.mutableExprPool.get();
            parseExpression(expr, false);
            ExprTree innerTree = expr.getInnerTree();
            return innerTree;
        }
    }

    /*--------------------------------------------------------------------
    *
    * Private Functions
    *
    *-------------------------------------------------------------------*/

    // Expression .= LogicalORExpression
    // | LogicalORExpression '?' Expression ':' Expression

    private boolean parseExpression(ExprTreeHolder tree) throws IOException {
        return parseExpression(tree, false);
    }

    private boolean parseExpression(ExprTreeHolder tree, boolean full) throws IOException {
        TokenType tt;
        if (!parseLogicalORExpression(tree)) {
            return false;
        }
        if ((tt = lexer.peekToken()) == TokenType.LEX_QMARK) {
            lexer.consumeToken();
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeM = objectPool.mutableExprPool.get();
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            parseExpression(treeM);
            if ((tt = lexer.consumeToken()) != TokenType.LEX_COLON) {
                throw new HyracksDataException("expected LEX_COLON, but got " + Lexer.strLexToken(tt));
            }
            parseExpression(treeR);
            if (treeL.getInnerTree() != null && treeM.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_TERNARY_OP, treeL, treeM, treeR, newTree);
                tree.setInnerTree(newTree);
                return (true);
            }
            tree.setInnerTree(null);
            return false;
        }
        // if a full parse was requested, ensure that input is exhausted
        if (full && (lexer.consumeToken() != TokenType.LEX_END_OF_INPUT)) {
            throw new HyracksDataException(
                    "expected LEX_END_OF_INPUT on full parse, but got " + String.valueOf(Lexer.strLexToken(tt)));
        }
        return true;
    }

    // LogicalORExpression .= LogicalANDExpression
    // | LogicalORExpression '||' LogicalANDExpression

    private boolean parseLogicalORExpression(ExprTreeHolder tree) throws IOException {
        if (!parseLogicalANDExpression(tree)) {
            return false;
        }
        while ((lexer.peekToken()) == TokenType.LEX_LOGICAL_OR) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseLogicalANDExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_LOGICAL_OR_OP, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
        }
        return true;
    }

    // LogicalANDExpression .= InclusiveORExpression
    // | LogicalANDExpression '&&' InclusiveORExpression
    private boolean parseLogicalANDExpression(ExprTreeHolder tree) throws IOException {
        if (!parseInclusiveORExpression(tree)) {
            return false;
        }
        while ((lexer.peekToken()) == TokenType.LEX_LOGICAL_AND) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseInclusiveORExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_LOGICAL_AND_OP, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
        }
        return true;
    }

    // InclusiveORExpression .= ExclusiveORExpression
    // | InclusiveORExpression '|' ExclusiveORExpression
    public boolean parseInclusiveORExpression(ExprTreeHolder tree) throws IOException {
        if (!parseExclusiveORExpression(tree)) {
            return false;
        }
        while ((lexer.peekToken()) == TokenType.LEX_BITWISE_OR) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseExclusiveORExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_BITWISE_OR_OP, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
        }
        return true;
    }

    // ExclusiveORExpression .= ANDExpression
    // | ExclusiveORExpression '^' ANDExpression
    private boolean parseExclusiveORExpression(ExprTreeHolder tree) throws IOException {
        if (!parseANDExpression(tree)) {
            return false;
        }
        while ((lexer.peekToken()) == TokenType.LEX_BITWISE_XOR) {
            lexer.consumeToken();
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            parseANDExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_BITWISE_XOR_OP, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
        }
        return true;
    }

    // ANDExpression .= EqualityExpression
    // | ANDExpression '&' EqualityExpression
    private boolean parseANDExpression(ExprTreeHolder tree) throws IOException {
        if (!parseEqualityExpression(tree)) {
            return false;
        }
        while ((lexer.peekToken()) == TokenType.LEX_BITWISE_AND) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseEqualityExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(Operation.OpKind_BITWISE_AND_OP, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
        }
        return true;
    }

    // EqualityExpression .= RelationalExpression
    // | EqualityExpression '==' RelationalExpression
    // | EqualityExpression '!=' RelationalExpression
    // | EqualityExpression '=?=' RelationalExpression
    // | EqualityExpression '=!=' RelationalExpression
    private boolean parseEqualityExpression(ExprTreeHolder tree) throws IOException {
        TokenType tt;
        int op = Operation.OpKind_NO_OP;
        if (!parseRelationalExpression(tree)) {
            return false;
        }
        tt = lexer.peekToken();
        while (tt == TokenType.LEX_EQUAL || tt == TokenType.LEX_NOT_EQUAL || tt == TokenType.LEX_META_EQUAL
                || tt == TokenType.LEX_META_NOT_EQUAL) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseRelationalExpression(treeR);
            switch (tt) {
                case LEX_EQUAL:
                    op = Operation.OpKind_EQUAL_OP;
                    break;
                case LEX_NOT_EQUAL:
                    op = Operation.OpKind_NOT_EQUAL_OP;
                    break;
                case LEX_META_EQUAL:
                    op = Operation.OpKind_META_EQUAL_OP;
                    break;
                case LEX_META_NOT_EQUAL:
                    op = Operation.OpKind_META_NOT_EQUAL_OP;
                    break;
                default:
                    throw new HyracksDataException("ClassAd:  Should not reach here");
            }
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(op, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
            tt = lexer.peekToken();
        }
        return true;
    }

    // RelationalExpression .= ShiftExpression
    // | RelationalExpression '<' ShiftExpression
    // | RelationalExpression '>' ShiftExpression
    // | RelationalExpression '<=' ShiftExpression
    // | RelationalExpression '>=' ShiftExpression
    private boolean parseRelationalExpression(ExprTreeHolder tree) throws IOException {
        TokenType tt;
        if (!parseShiftExpression(tree)) {
            return false;
        }
        tt = lexer.peekToken();
        while (tt == TokenType.LEX_LESS_THAN || tt == TokenType.LEX_GREATER_THAN || tt == TokenType.LEX_LESS_OR_EQUAL
                || tt == TokenType.LEX_GREATER_OR_EQUAL) {
            int op = Operation.OpKind_NO_OP;
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseShiftExpression(treeR);
            switch (tt) {
                case LEX_LESS_THAN:
                    op = Operation.OpKind_LESS_THAN_OP;
                    break;
                case LEX_LESS_OR_EQUAL:
                    op = Operation.OpKind_LESS_OR_EQUAL_OP;
                    break;
                case LEX_GREATER_THAN:
                    op = Operation.OpKind_GREATER_THAN_OP;
                    break;
                case LEX_GREATER_OR_EQUAL:
                    op = Operation.OpKind_GREATER_OR_EQUAL_OP;
                    break;
                default:
                    throw new HyracksDataException("ClassAd:  Should not reach here");
            }
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(op, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
            tt = lexer.peekToken();
        }
        return true;
    }

    // ShiftExpression .= AdditiveExpression
    // | ShiftExpression '<<' AdditiveExpression
    // | ShiftExpression '>>' AdditiveExpression
    // | ShiftExpression '>>>' AditiveExpression
    private boolean parseShiftExpression(ExprTreeHolder tree) throws IOException {
        if (!parseAdditiveExpression(tree)) {
            return false;
        }

        TokenType tt = lexer.peekToken();
        while (tt == TokenType.LEX_LEFT_SHIFT || tt == TokenType.LEX_RIGHT_SHIFT || tt == TokenType.LEX_URIGHT_SHIFT) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            int op;
            lexer.consumeToken();
            parseAdditiveExpression(treeR);
            switch (tt) {
                case LEX_LEFT_SHIFT:
                    op = Operation.OpKind_LEFT_SHIFT_OP;
                    break;
                case LEX_RIGHT_SHIFT:
                    op = Operation.OpKind_RIGHT_SHIFT_OP;
                    break;
                case LEX_URIGHT_SHIFT:
                    op = Operation.OpKind_URIGHT_SHIFT_OP;
                    break;
                default:
                    op = Operation.OpKind_NO_OP; // Make gcc's -wuninitalized happy
                    throw new HyracksDataException("ClassAd:  Should not reach here");
            }

            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(op, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
            tt = lexer.peekToken();
        }
        return true;
    }

    // AdditiveExpression .= MultiplicativeExpression
    // | AdditiveExpression '+' MultiplicativeExpression
    // | AdditiveExpression '-' MultiplicativeExpression
    private boolean parseAdditiveExpression(ExprTreeHolder tree) throws IOException {
        if (!parseMultiplicativeExpression(tree)) {
            return false;
        }

        TokenType tt = lexer.peekToken();
        while (tt == TokenType.LEX_PLUS || tt == TokenType.LEX_MINUS) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            lexer.consumeToken();
            parseMultiplicativeExpression(treeR);
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(
                        (tt == TokenType.LEX_PLUS) ? Operation.OpKind_ADDITION_OP : Operation.OpKind_SUBTRACTION_OP,
                        treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
            tt = lexer.peekToken();
        }
        return true;
    }

    // MultiplicativeExpression .= UnaryExpression
    // | MultiplicativeExpression '*' UnaryExpression
    // | MultiplicativeExpression '/' UnaryExpression
    // | MultiplicativeExpression '%' UnaryExpression
    private boolean parseMultiplicativeExpression(ExprTreeHolder tree) throws IOException {
        if (!parseUnaryExpression(tree)) {
            return false;
        }

        TokenType tt = lexer.peekToken();
        while (tt == TokenType.LEX_MULTIPLY || tt == TokenType.LEX_DIVIDE || tt == TokenType.LEX_MODULUS) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            int op;
            lexer.consumeToken();
            parseUnaryExpression(treeR);
            switch (tt) {
                case LEX_MULTIPLY:
                    op = Operation.OpKind_MULTIPLICATION_OP;
                    break;
                case LEX_DIVIDE:
                    op = Operation.OpKind_DIVISION_OP;
                    break;
                case LEX_MODULUS:
                    op = Operation.OpKind_MODULUS_OP;
                    break;
                default:
                    op = Operation.OpKind_NO_OP; // Make gcc's -wuninitalized happy
                    throw new HyracksDataException("ClassAd:  Should not reach here");
            }
            if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(op, treeL, treeR, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return false;
            }
            tt = lexer.peekToken();
        }
        return true;
    }

    // UnaryExpression .= PostfixExpression
    // | UnaryOperator UnaryExpression
    // ( where UnaryOperator is one of { -, +, ~, ! } )
    private boolean parseUnaryExpression(ExprTreeHolder tree) throws IOException {
        TokenType tt = lexer.peekToken();
        if (tt == TokenType.LEX_MINUS || tt == TokenType.LEX_PLUS || tt == TokenType.LEX_BITWISE_NOT
                || tt == TokenType.LEX_LOGICAL_NOT) {
            lexer.consumeToken();
            ExprTreeHolder treeM = objectPool.mutableExprPool.get();
            int op = Operation.OpKind_NO_OP;
            parseUnaryExpression(treeM);
            switch (tt) {
                case LEX_MINUS:
                    op = Operation.OpKind_UNARY_MINUS_OP;
                    break;
                case LEX_PLUS:
                    op = Operation.OpKind_UNARY_PLUS_OP;
                    break;
                case LEX_BITWISE_NOT:
                    op = Operation.OpKind_BITWISE_NOT_OP;
                    break;
                case LEX_LOGICAL_NOT:
                    op = Operation.OpKind_LOGICAL_NOT_OP;
                    break;
                default:
                    throw new HyracksDataException("ClassAd: Shouldn't Get here");
            }
            if (treeM.getInnerTree() != null) {
                Operation newTree = objectPool.operationPool.get();
                Operation.createOperation(op, treeM, null, null, newTree);
                tree.setInnerTree(newTree);
            } else {
                tree.setInnerTree(null);
                return (false);
            }
            return true;
        } else {
            return parsePostfixExpression(tree);
        }
    }

    // PostfixExpression .= PrimaryExpression
    // | PostfixExpression '.' Identifier
    // | PostfixExpression '[' Expression ']'
    private boolean parsePostfixExpression(ExprTreeHolder tree) throws IOException {
        TokenType tt;
        if (!parsePrimaryExpression(tree)) {
            return false;
        }
        while ((tt = lexer.peekToken()) == TokenType.LEX_OPEN_BOX || tt == TokenType.LEX_SELECTION) {
            ExprTreeHolder treeL = tree;
            ExprTreeHolder treeR = objectPool.mutableExprPool.get();
            TokenValue tv = objectPool.tokenValuePool.get();
            lexer.consumeToken();
            if (tt == TokenType.LEX_OPEN_BOX) {
                // subscript operation
                parseExpression(treeR);
                if (treeL.getInnerTree() != null && treeR.getInnerTree() != null) {
                    Operation newTree = objectPool.operationPool.get();
                    Operation.createOperation(Operation.OpKind_SUBSCRIPT_OP, treeL, treeR, null, newTree);
                    if (lexer.consumeToken() == TokenType.LEX_CLOSE_BOX) {
                        tree.setInnerTree(newTree);
                        continue;
                    }
                }
                tree.setInnerTree(null);
                return false;
            } else if (tt == TokenType.LEX_SELECTION) {
                // field selection operation
                if ((tt = lexer.consumeToken(tv)) != TokenType.LEX_IDENTIFIER) {
                    throw new HyracksDataException("second argument of selector must be an " + "identifier (got"
                            + String.valueOf(Lexer.strLexToken(tt)) + ")");
                }
                AttributeReference newTree = objectPool.attrRefPool.get();
                AttributeReference.createAttributeReference(treeL, tv.getStrValue(), false, newTree);
                tree.setInnerTree(newTree);
            }
        }
        return true;
    }

    // PrimaryExpression .= Identifier
    // | FunctionCall
    // | '.' Identifier
    // | '(' Expression ')'
    // | Literal
    // FunctionCall .= Identifier ArgumentList
    // ( Constant may be
    // boolean,undefined,error,string,integer,real,classad,list )
    // ( ArgumentList non-terminal includes parentheses )
    private boolean parsePrimaryExpression(ExprTreeHolder tree) throws IOException {
        ExprTreeHolder treeL;
        TokenValue tv = objectPool.tokenValuePool.get();
        TokenType tt;
        tree.setInnerTree(null);
        switch ((tt = lexer.peekToken(tv))) {
            // identifiers
            case LEX_IDENTIFIER:
                isExpr = true;
                lexer.consumeToken();
                // check for funcion call
                if ((tt = lexer.peekToken()) == TokenType.LEX_OPEN_PAREN) {
                    ExprList argList = objectPool.exprListPool.get();
                    if (!parseArgumentList(argList)) {
                        tree.setInnerTree(null);
                        return false;
                    }
                    // special case function-calls should be converted
                    // into a literal expression if the argument is a
                    // string literal
                    if (shouldEvaluateAtParseTime(tv.getStrValue().toString(), argList)) {
                        tree.setInnerTree(evaluateFunction(tv.getStrValue().toString(), argList));
                    } else {
                        tree.setInnerTree(
                                FunctionCall.createFunctionCall(tv.getStrValue().toString(), argList, objectPool));
                    }
                } else {
                    // I don't think this is ever hit
                    tree.setInnerTree(
                            AttributeReference.createAttributeReference(null, tv.getStrValue(), false, objectPool));
                }
                return (tree.getInnerTree() != null);
            case LEX_SELECTION:
                isExpr = true;
                lexer.consumeToken();
                if ((tt = lexer.consumeToken(tv)) == TokenType.LEX_IDENTIFIER) {
                    // the boolean final arg signifies that reference is absolute
                    tree.setInnerTree(
                            AttributeReference.createAttributeReference(null, tv.getStrValue(), true, objectPool));
                    return (tree.size() != 0);
                }
                // not an identifier following the '.'
                throw new HyracksDataException(
                        "need identifier in selection expression (got" + Lexer.strLexToken(tt) + ")");
                // parenthesized expression
            case LEX_OPEN_PAREN: {
                isExpr = true;
                lexer.consumeToken();
                treeL = objectPool.mutableExprPool.get();
                parseExpression(treeL);
                if (treeL.getInnerTree() == null) {
                    tree.resetExprTree(null);
                    return false;
                }

                if ((tt = lexer.consumeToken()) != TokenType.LEX_CLOSE_PAREN) {
                    throw new HyracksDataException("exptected LEX_CLOSE_PAREN, but got " + Lexer.strLexToken(tt));
                    // tree.resetExprTree(null);
                    // return false;
                }
                // assume make operation will return a new tree
                tree.setInnerTree(Operation.createOperation(Operation.OpKind_PARENTHESES_OP, treeL, objectPool));
                return (tree.size() != 0);
            }
            // constants
            case LEX_OPEN_BOX: {
                isExpr = true;
                ClassAd newAd = objectPool.classAdPool.get();
                if (!parseClassAd(newAd)) {
                    tree.resetExprTree(null);
                    return false;
                }
                tree.setInnerTree(newAd);
            }
                return true;

            case LEX_OPEN_BRACE: {
                isExpr = true;
                ExprList newList = objectPool.exprListPool.get();
                if (!parseExprList(newList)) {
                    tree.setInnerTree(null);
                    return false;
                }
                tree.setInnerTree(newList);
            }
                return true;

            case LEX_UNDEFINED_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setUndefinedValue();
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }
            case LEX_ERROR_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setErrorValue();
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }
            case LEX_BOOLEAN_VALUE: {
                Value val = objectPool.valuePool.get();
                MutableBoolean b = new MutableBoolean();
                tv.getBoolValue(b);
                lexer.consumeToken();
                val.setBooleanValue(b);
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }

            case LEX_INTEGER_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setIntegerValue(tv.getIntValue());
                tree.setInnerTree(Literal.createLiteral(val, tv.getFactor(), objectPool));
                return (tree.getInnerTree() != null);
            }

            case LEX_REAL_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setRealValue(tv.getRealValue());
                tree.setInnerTree(Literal.createLiteral(val, tv.getFactor(), objectPool));
                return (tree.getInnerTree() != null);
            }

            case LEX_STRING_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setStringValue(tv.getStrValue());
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }

            case LEX_ABSOLUTE_TIME_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setAbsoluteTimeValue(tv.getTimeValue());
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }

            case LEX_RELATIVE_TIME_VALUE: {
                Value val = objectPool.valuePool.get();
                lexer.consumeToken();
                val.setRelativeTimeValue(tv.getTimeValue().getRelativeTime());
                tree.setInnerTree(Literal.createLiteral(val, objectPool));
                return (tree.getInnerTree() != null);
            }

            default:
                tree.setInnerTree(null);
                return false;
        }
    }

    // ArgumentList .= '(' ListOfArguments ')'
    // ListOfArguments .= (epsilon)
    // | ListOfArguments ',' Expression
    public boolean parseArgumentList(ExprList argList) throws IOException {
        TokenType tt;
        argList.clear();
        if ((tt = lexer.consumeToken()) != TokenType.LEX_OPEN_PAREN) {
            throw new HyracksDataException("expected LEX_OPEN_PAREN but got " + String.valueOf(Lexer.strLexToken(tt)));
            // return false;
        }
        tt = lexer.peekToken();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        while (tt != TokenType.LEX_CLOSE_PAREN) {
            // parse the expression
            tree.reset();
            parseExpression(tree);
            if (tree.getInnerTree() == null) {
                argList.clear();
                return false;
            }
            // insert the expression into the argument list
            argList.add(tree.getInnerTree());
            // the next token must be a ',' or a ')'
            // or it can be a ';' if using old ClassAd semantics
            tt = lexer.peekToken();
            if (tt == TokenType.LEX_COMMA || (tt == TokenType.LEX_SEMICOLON && false)) {
                lexer.consumeToken();
            } else if (tt != TokenType.LEX_CLOSE_PAREN) {
                argList.clear();
                throw new HyracksDataException(
                        "expected LEX_COMMA or LEX_CLOSE_PAREN but got " + String.valueOf(Lexer.strLexToken(tt)));
                // return false;
            }
        }
        lexer.consumeToken();
        return true;
    }

    // ClassAd .= '[' AttributeList ']'
    // AttributeList .= (epsilon)
    // | Attribute ';' AttributeList
    // Attribute .= Identifier '=' Expression
    public boolean parseClassAd(ClassAd ad) throws IOException {
        return parseClassAd(ad, false);
    }

    public boolean parseClassAdOld(ClassAd ad, boolean full) throws IOException {
        return false;
    }

    public boolean parseClassAd(ClassAd ad, boolean full) throws IOException {
        TokenType tt;
        ad.clear();
        if ((tt = lexer.consumeToken()) != TokenType.LEX_OPEN_BOX) {
            return false;
        }
        tt = lexer.peekToken();
        TokenValue tv = objectPool.tokenValuePool.get();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        while (tt != TokenType.LEX_CLOSE_BOX) {
            // Get the name of the expression
            tv.reset();
            tree.reset();
            tt = lexer.consumeToken(tv);
            if (tt == TokenType.LEX_SEMICOLON) {
                // We allow empty expressions, so if someone give a double
                // semicolon, it doesn't
                // hurt. Technically it's not right, but we shouldn't make users
                // pay the price for
                // a meaningless mistake. See condor-support #1881 for a user
                // that was bitten by this.
                continue;
            }
            if (tt != TokenType.LEX_IDENTIFIER) {
                throw new HyracksDataException(
                        "while parsing classad:  expected LEX_IDENTIFIER " + " but got " + Lexer.strLexToken(tt));
            }

            // consume the intermediate '='
            if ((tt = lexer.consumeToken()) != TokenType.LEX_BOUND_TO) {
                throw new HyracksDataException(
                        "while parsing classad:  expected LEX_BOUND_TO " + " but got " + Lexer.strLexToken(tt));
            }

            isExpr = false;
            parseExpression(tree);
            if (tree.getInnerTree() == null) {
                throw new HyracksDataException("parse expression returned empty tree");
            }

            // insert the attribute into the classad
            if (!ad.insert(tv.getStrValue().toString(), tree)) {
                throw new HyracksDataException("Couldn't insert value to classad");
            }

            // the next token must be a ';' or a ']'
            tt = lexer.peekToken();
            if (tt != TokenType.LEX_SEMICOLON && tt != TokenType.LEX_CLOSE_BOX) {
                throw new HyracksDataException("while parsing classad:  expected LEX_SEMICOLON or "
                        + "LEX_CLOSE_BOX but got " + Lexer.strLexToken(tt));
            }

            // Slurp up any extra semicolons. This does not duplicate the work
            // at the top of the loop
            // because it accounts for the case where the last expression has
            // extra semicolons,
            // while the first case accounts for optional beginning semicolons.
            while (tt == TokenType.LEX_SEMICOLON) {
                lexer.consumeToken();
                tt = lexer.peekToken();
            }
        }
        lexer.consumeToken();
        // if a full parse was requested, ensure that input is exhausted
        if (full && (lexer.consumeToken() != TokenType.LEX_END_OF_INPUT)) {
            throw new HyracksDataException("while parsing classad:  expected LEX_END_OF_INPUT for "
                    + "full parse but got " + Lexer.strLexToken(tt));
        }
        return true;
    }

    // ExprList .= '{' ListOfExpressions '}'
    // ListOfExpressions .= (epsilon)
    // | Expression ',' ListOfExpressions
    public boolean parseExprList(ExprList list) throws IOException {
        return parseExprList(list, false);
    }

    public boolean parseExprList(ExprList list, boolean full) throws IOException {
        TokenType tt;
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        ExprList loe = objectPool.exprListPool.get();

        if ((tt = lexer.consumeToken()) != TokenType.LEX_OPEN_BRACE) {
            throw new HyracksDataException(
                    "while parsing expression list:  expected LEX_OPEN_BRACE" + " but got " + Lexer.strLexToken(tt));
            // return false;
        }
        tt = lexer.peekToken();
        while (tt != TokenType.LEX_CLOSE_BRACE) {
            // parse the expression
            parseExpression(tree);
            if (tree.getInnerTree() == null) {
                throw new HyracksDataException("while parsing expression list:  expected "
                        + "LEX_CLOSE_BRACE or LEX_COMMA but got " + Lexer.strLexToken(tt));
            }

            // insert the expression into the list
            loe.add(tree);

            // the next token must be a ',' or a '}'
            tt = lexer.peekToken();
            if (tt == TokenType.LEX_COMMA) {
                lexer.consumeToken();
            } else if (tt != TokenType.LEX_CLOSE_BRACE) {
                throw new HyracksDataException("while parsing expression list:  expected "
                        + "LEX_CLOSE_BRACE or LEX_COMMA but got " + Lexer.strLexToken(tt));
            }
        }

        lexer.consumeToken();
        list.setValue(ExprList.createExprList(loe, objectPool));

        // if a full parse was requested, ensure that input is exhausted
        if (full && (lexer.consumeToken() != TokenType.LEX_END_OF_INPUT)) {
            list.clear();
            throw new HyracksDataException("while parsing expression list:  expected "
                    + "LEX_END_OF_INPUT for full parse but got " + Lexer.strLexToken(tt));
        }
        return true;
    }

    public boolean shouldEvaluateAtParseTime(String functionName, ExprList argList) throws HyracksDataException {
        boolean should_eval = false;
        if (functionName.equalsIgnoreCase("absTime") || functionName.equalsIgnoreCase("relTime")) {
            if (argList.size() == 1 && argList.get(0).getKind() == NodeKind.LITERAL_NODE) {
                Value val = objectPool.valuePool.get();
                AMutableNumberFactor factor = objectPool.numFactorPool.get();
                ((Literal) argList.get(0)).getComponents(val, factor);
                if (val.isStringValue()) {
                    should_eval = true;
                }
            }
        }
        return should_eval;
    }

    public ExprTree evaluateFunction(String functionName, ExprList argList) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        AMutableNumberFactor factor = objectPool.numFactorPool.get();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        ((Literal) argList.get(0)).getComponents(val, factor);

        AMutableCharArrayString string_value = objectPool.strPool.get();
        if (val.isStringValue(string_value)) {
            if (functionName.equalsIgnoreCase("absTime")) {
                tree.setInnerTree(Literal.createAbsTime(string_value, objectPool));
            } else if (functionName.equalsIgnoreCase("relTime")) {
                tree.setInnerTree(Literal.createRelTime(string_value, objectPool));
            } else {
                tree.setInnerTree(FunctionCall.createFunctionCall(functionName, argList, objectPool));
            }
        } else {
            tree.setInnerTree(FunctionCall.createFunctionCall(functionName, argList, objectPool));
        }
        return tree;
    }

    public TokenType peekToken() throws IOException {
        if (lexer.wasInitialized()) {
            return lexer.peekToken();
        } else {
            return TokenType.LEX_TOKEN_ERROR;
        }
    }

    public TokenType consumeToken() throws IOException {
        if (lexer.wasInitialized()) {
            return lexer.consumeToken();
        } else {
            return TokenType.LEX_TOKEN_ERROR;
        }
    }

    public boolean parseExpression(String buf, ExprTreeHolder tree) throws IOException {
        return parseExpression(buf, tree, false);
    }

    public boolean parseExpression(String buf, ExprTreeHolder tree, boolean full) throws IOException {
        boolean success;
        StringLexerSource lexer_source = new StringLexerSource(buf);

        success = false;
        if (lexer.initialize(lexer_source)) {
            success = parseExpression(tree, full);
        }
        return success;
    }

    public ClassAd parseClassAd(String input_basic) throws IOException {
        return parseClassAd(input_basic, false);
    }

    public LexerSource getLexerSource() {
        return currentSource;
    }

    public void setLexerSource(LexerSource lexerSource) {
        this.currentSource = lexerSource;
    }

    @Override
    public void parse(IRawRecord<? extends char[]> record, DataOutput out) throws HyracksDataException {
        try {
            resetPools();
            if (oldFormat) {
                int maxOffset = record.size();
                rootAd.clear();
                char[] buffer = record.get();
                aInt32.setValue(0);
                String line = readLine(buffer, aInt32, maxOffset);
                while (line != null) {
                    if (line.trim().length() == 0) {
                        if (rootAd.size() == 0) {
                            line = readLine(buffer, aInt32, maxOffset);
                            continue;
                        }
                        break;
                    } else if (!rootAd.insert(line)) {
                        throw new HyracksDataException("Couldn't parse expression in line: " + line);
                    }
                    line = readLine(buffer, aInt32, maxOffset);
                }
            } else {
                currentSource.setNewSource(record.get());
                rootAd.reset();
                asterixParseClassAd(rootAd);
            }
            parseRecord(recordType, rootAd, out);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
