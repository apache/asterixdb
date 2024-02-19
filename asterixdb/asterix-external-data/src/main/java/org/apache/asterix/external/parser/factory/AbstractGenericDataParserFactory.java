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
package org.apache.asterix.external.parser.factory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;

public abstract class AbstractGenericDataParserFactory<T> extends AbstractRecordStreamParserFactory<T> {
    private static final long serialVersionUID = 1L;
    private static final List<ATypeTag> UNSUPPORTED_TYPES = Collections
            .unmodifiableList(Arrays.asList(ATypeTag.MULTISET, ATypeTag.POINT3D, ATypeTag.CIRCLE, ATypeTag.RECTANGLE,
                    ATypeTag.INTERVAL, ATypeTag.DAYTIMEDURATION, ATypeTag.DURATION, ATypeTag.BINARY));

    @Override
    public void setRecordType(ARecordType recordType) throws AsterixException {
        checkRecordTypeCompatibility(recordType);
        super.setRecordType(recordType);
    }

    /**
     * Check if the defined type contains ADM special types.
     * if it contains unsupported types.
     *
     * @param recordType
     * @throws AsterixException
     */
    private void checkRecordTypeCompatibility(ARecordType recordType) throws AsterixException {
        final IAType[] fieldTypes = recordType.getFieldTypes();
        for (IAType type : fieldTypes) {
            checkTypeCompatibility(type);
        }
    }

    private void checkTypeCompatibility(IAType type) throws AsterixException {
        if (UNSUPPORTED_TYPES.contains(type.getTypeTag())) {
            throw new AsterixException(ErrorCode.TYPE_UNSUPPORTED, JSONDataParserFactory.class.getName(),
                    type.getTypeTag().toString());
        } else if (type.getTypeTag() == ATypeTag.ARRAY) {
            checkTypeCompatibility(((AOrderedListType) type).getItemType());
        } else if (type.getTypeTag() == ATypeTag.OBJECT) {
            checkRecordTypeCompatibility((ARecordType) type);
        } else if (type.getTypeTag() == ATypeTag.UNION) {
            checkTypeCompatibility(((AUnionType) type).getActualType());
        }
        //Compatible type
    }

}
