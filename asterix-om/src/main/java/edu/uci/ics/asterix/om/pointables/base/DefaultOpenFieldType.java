/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.pointables.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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
        try {
            NESTED_OPEN_RECORD_TYPE = new ARecordType("nested-open", new String[] {}, new IAType[] {}, true);
        } catch (AsterixException e) {
            throw new AsterixRuntimeException();
        }
    }

    // nested open list type
    public static AOrderedListType NESTED_OPEN_AORDERED_LIST_TYPE = new AOrderedListType(BuiltinType.ANY,
            "nested-ordered-list");

    // nested open list type
    public static AUnorderedListType NESTED_OPEN_AUNORDERED_LIST_TYPE = new AUnorderedListType(BuiltinType.ANY,
            "nested-unordered-list");

    public static IAType getDefaultOpenFieldType(ATypeTag tag) {
        if (tag.equals(ATypeTag.RECORD))
            return NESTED_OPEN_RECORD_TYPE;
        if (tag.equals(ATypeTag.ORDEREDLIST))
            return NESTED_OPEN_AORDERED_LIST_TYPE;
        if (tag.equals(ATypeTag.UNORDEREDLIST))
            return NESTED_OPEN_AUNORDERED_LIST_TYPE;
        else
            return null;
    }

}
