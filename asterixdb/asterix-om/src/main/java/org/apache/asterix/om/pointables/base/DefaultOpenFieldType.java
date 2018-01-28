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

package org.apache.asterix.om.pointables.base;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * This class serves as the repository for the default record type and list type
 * fields in the open part, e.g., a "record" (nested) field in the open part is
 * always a fully open one, and a "list" field in the open part is always a list
 * of "ANY".
 */
public class DefaultOpenFieldType {

    // nested open field rec type
    public static ARecordType NESTED_OPEN_RECORD_TYPE;

    static {
        NESTED_OPEN_RECORD_TYPE = new ARecordType("nested-open", new String[] {}, new IAType[] {}, true);
    }

    // nested open list type
    public static AOrderedListType NESTED_OPEN_AORDERED_LIST_TYPE =
            new AOrderedListType(BuiltinType.ANY, "nested-ordered-list");

    // nested open list type
    public static AUnorderedListType NESTED_OPEN_AUNORDERED_LIST_TYPE =
            new AUnorderedListType(BuiltinType.ANY, "nested-unordered-list");

    public static IAType getDefaultOpenFieldType(ATypeTag tag) {
        if (tag.equals(ATypeTag.OBJECT))
            return NESTED_OPEN_RECORD_TYPE;
        if (tag.equals(ATypeTag.ARRAY))
            return NESTED_OPEN_AORDERED_LIST_TYPE;
        if (tag.equals(ATypeTag.MULTISET))
            return NESTED_OPEN_AUNORDERED_LIST_TYPE;
        else
            return null;
    }

}
