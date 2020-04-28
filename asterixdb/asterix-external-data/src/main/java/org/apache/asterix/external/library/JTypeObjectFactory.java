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

import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.base.JBoolean;
import org.apache.asterix.external.library.java.base.JDate;
import org.apache.asterix.external.library.java.base.JDateTime;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JDuration;
import org.apache.asterix.external.library.java.base.JFloat;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JInterval;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JMissing;
import org.apache.asterix.external.library.java.base.JNull;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.external.library.java.base.JTime;
import org.apache.asterix.external.library.java.base.JUnorderedList;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;

public class JTypeObjectFactory implements IObjectFactory<IJObject, IAType> {

    public static final JTypeObjectFactory INSTANCE = new JTypeObjectFactory();

    private JTypeObjectFactory() {
    }

    @Override
    public IJObject create(IAType type) {
        IJObject retValue = null;
        switch (type.getTypeTag()) {
            case INTEGER:
                retValue = new JInt(0);
                break;
            case STRING:
                retValue = new JString("");
                break;
            case FLOAT:
                retValue = new JFloat(0);
                break;
            case DOUBLE:
                retValue = new JDouble(0);
                break;
            case BOOLEAN:
                retValue = new JBoolean(false);
                break;
            case DATE:
                retValue = new JDate(0);
                break;
            case DATETIME:
                retValue = new JDateTime(0);
                break;
            case DURATION:
                retValue = new JDuration(0, 0);
                break;
            case INTERVAL:
                retValue = new JInterval(0, 0);
                break;
            case TIME:
                retValue = new JTime(0);
                break;
            case BIGINT:
                retValue = new JLong(0);
                break;
            case NULL:
                retValue = JNull.INSTANCE;
                break;
            case MISSING:
                retValue = JMissing.INSTANCE;
                break;
            case ARRAY:
                AOrderedListType ot = (AOrderedListType) type;
                IAType orderedItemType = ot.getItemType();
                retValue = new JOrderedList(orderedItemType);
                break;
            case MULTISET:
                AUnorderedListType ut = (AUnorderedListType) type;
                IAType unorderedItemType = ut.getItemType();
                retValue = new JUnorderedList(unorderedItemType);
                break;
            case OBJECT:
                IAType[] fieldTypes = ((ARecordType) type).getFieldTypes();
                IJObject[] fieldObjects = null;
                if (fieldTypes != null) {
                    fieldObjects = new IJObject[fieldTypes.length];
                    int index = 0;
                    for (IAType fieldType : fieldTypes) {
                        fieldObjects[index] = create(fieldType);
                        index++;
                    }
                    retValue = new JRecord((ARecordType) type, fieldObjects);
                } else {
                    retValue = new JRecord((ARecordType) type, new IJObject[] {});
                }
                break;
            case UNION:
                AUnionType unionType = (AUnionType) type;
                IJObject itemObject = null;
                if (unionType.isMissableType()) {
                    itemObject = create(unionType);
                }
                retValue = itemObject;
                break;
            default:
                break;
        }
        return retValue;
    }

}
