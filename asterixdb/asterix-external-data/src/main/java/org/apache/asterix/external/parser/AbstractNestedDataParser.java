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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Abstract class for nested formats (ADM, JSON, XML ... etc)
 * TODO(wyk): remove extends AbstractDataParser and only take what's needed from it.
 * TODO(wyk): find a way to support ADM constructors for ADMDataParser
 */
public abstract class AbstractNestedDataParser<T> extends AbstractDataParser {

    private T currentParsedToken;

    /**
     * Parse object using the defined recordType.
     *
     * @param recordType
     *            {@value RecordUtil.FULLY_OPEN_RECORD_TYPE} if parsing open object
     * @param out
     * @throws HyracksDataException
     */
    protected abstract void parseObject(ARecordType recordType, DataOutput out) throws IOException;

    /**
     * Parse array using the defined listType.
     *
     * NOTE: currently AsterixDB only supports null values for open collection types.
     *
     * @param recordType
     *            {@value AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE} if parsing open array
     * @param out
     * @throws HyracksDataException
     */
    protected abstract void parseArray(AOrderedListType listType, DataOutput out) throws IOException;

    /**
     * Parse multiset using the defined listType.
     *
     * NOTE: currently AsterixDB only supports null values for open collection types.
     *
     * @param recordType
     *            {@value AUnorderedListType.FULLY_OPEN_UNORDEREDLIST_TYPE} if parsing open multiset
     * @param out
     * @throws HyracksDataException
     */
    protected abstract void parseMultiset(AUnorderedList listType, DataOutput out) throws IOException;

    /**
     * Map the third-party parser's token to {@link T}
     * This method is called by nextToken to set {@link AbstractNestedDataParser#currentParsedToken}
     *
     * @return the corresponding token
     * @throws IOException
     */
    protected abstract T advanceToNextToken() throws IOException;

    public final T nextToken() throws IOException {
        currentParsedToken = advanceToNextToken();
        return currentParsedToken;
    }

    public final T currentToken() {
        return currentParsedToken;
    }

    protected boolean isNullableType(IAType definedType) {
        if (definedType.getTypeTag() != ATypeTag.UNION) {
            return false;
        }

        return ((AUnionType) definedType).isNullableType();
    }

    protected boolean isMissableType(IAType definedType) {
        if (definedType.getTypeTag() != ATypeTag.UNION) {
            return false;
        }

        return ((AUnionType) definedType).isMissableType();
    }

    protected void checkOptionalConstraints(ARecordType recordType, BitSet nullBitmap) throws RuntimeDataException {
        for (int i = 0; i < recordType.getFieldTypes().length; i++) {
            if (!nullBitmap.get(i) && !isMissableType(recordType.getFieldTypes()[i])) {
                throw new RuntimeDataException(ErrorCode.PARSER_TWEET_PARSER_CLOSED_FIELD_NULL,
                        recordType.getFieldNames()[i]);
            }
        }
    }

    /**
     * Parser is not expecting definedType to be null.
     *
     * @param definedType
     *            type defined by the user.
     * @param parsedTypeTag
     *            parsed type.
     * @return
     *         definedType is nullable && parsedTypeTag == ATypeTag.NULL => return ANULL
     *         definedType == ANY && isComplexType => fully_open_complex_type
     *         definedType == ANY && isAtomicType => ANY
     *         defiendType == parsedTypeTag | canBeConverted => return definedType
     * @throws RuntimeDataException
     *             type mismatch
     */
    protected IAType checkAndGetType(IAType definedType, ATypeTag parsedTypeTag) throws RuntimeDataException {
        //Cannot be missing
        if (parsedTypeTag == ATypeTag.NULL && isNullableType(definedType)) {
            return BuiltinType.ANULL;
        }

        final IAType actualDefinedType = getActualType(definedType);
        if (actualDefinedType.getTypeTag() == ATypeTag.ANY) {
            switch (parsedTypeTag) {
                case OBJECT:
                    return RecordUtil.FULLY_OPEN_RECORD_TYPE;
                case ARRAY:
                    return AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;
                case MULTISET:
                    return AUnorderedListType.FULLY_OPEN_UNORDEREDLIST_TYPE;
                default:
                    return BuiltinType.ANY;
            }
        } else if (actualDefinedType.getTypeTag() == parsedTypeTag
                || isConvertable(parsedTypeTag, actualDefinedType.getTypeTag())) {
            return actualDefinedType;
        }

        throw new RuntimeDataException(ErrorCode.PARSER_ADM_DATA_PARSER_TYPE_MISMATCH, definedType.getTypeName());
    }

    private IAType getActualType(IAType definedType) {
        if (definedType.getTypeTag() == ATypeTag.UNION) {
            return ((AUnionType) definedType).getActualType();
        }
        return definedType;
    }

    /**
     * Check promote/demote rules for mismatched types.
     * String type is a special case as it can be parsed as date/time/datetime/UUID
     *
     * @param parsedTypeTag
     * @param definedTypeTag
     * @return
     *         true if it can be converted
     *         false otherwise
     */
    protected boolean isConvertable(ATypeTag parsedTypeTag, ATypeTag definedTypeTag) {
        boolean convertable = parsedTypeTag == ATypeTag.STRING;

        convertable &= definedTypeTag == ATypeTag.UUID || definedTypeTag == ATypeTag.DATE
                || definedTypeTag == ATypeTag.TIME || definedTypeTag == ATypeTag.DATETIME;

        return convertable || ATypeHierarchy.canPromote(parsedTypeTag, definedTypeTag)
                || ATypeHierarchy.canDemote(parsedTypeTag, definedTypeTag);
    }

}
