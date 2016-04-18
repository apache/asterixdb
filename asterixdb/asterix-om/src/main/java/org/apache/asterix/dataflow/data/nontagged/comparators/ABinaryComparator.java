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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.dataflow.value.BinaryComparatorConstant.ComparableResultCode;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/*
 * Asterix-level comparators will extend this class so that they can execute isComparable() first before doing actual compare().
 */
public abstract class ABinaryComparator implements IBinaryComparator {

    public static ComparableResultCode isComparable(byte tag1,byte tag2) {
        // NULL Check. If one type is NULL, then we return NULL
        if (tag1 == ATypeTag.SERIALIZED_NULL_TYPE_TAG || tag2 == ATypeTag.SERIALIZED_NULL_TYPE_TAG || tag1 == 0
                || tag2 == 0) {
            return ComparableResultCode.UNKNOWN;
        }

        // Checks whether two types are comparable or not
        ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tag1);
        ATypeTag typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tag2);

        // Are two types compatible, meaning that they can be compared? (e.g., compare between numeric types
        if (ATypeHierarchy.isCompatible(typeTag1, typeTag2)) {
            return ComparableResultCode.TRUE;
        } else {
            return ComparableResultCode.FALSE;
        }

    }

    @Override
    public abstract int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException;

}
