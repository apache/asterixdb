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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;

public class TypeComputerUtils {

    public static ARecordType extractRecordType(IAType t) {
        if (t.getTypeTag() == ATypeTag.RECORD) {
            return (ARecordType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.RECORD) {
                return (ARecordType) innerType;
            }
        }

        return null;
    }

    public static AOrderedListType extractOrderedListType(IAType t) {
        if (t.getTypeTag() == ATypeTag.ORDEREDLIST) {
            return (AOrderedListType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.ORDEREDLIST) {
                return (AOrderedListType) innerType;
            }
        }

        return null;
    }

    public static AUnorderedListType extractUnorderedListType(IAType t) {
        if (t.getTypeTag() == ATypeTag.UNORDEREDLIST) {
            return (AUnorderedListType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getUnionList().get(1);
            if (innerType.getTypeTag() == ATypeTag.UNORDEREDLIST) {
                return (AUnorderedListType) innerType;
            }
        }

        return null;
    }

}
