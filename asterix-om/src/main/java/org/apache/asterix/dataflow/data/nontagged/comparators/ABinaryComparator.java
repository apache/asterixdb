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

    public static ComparableResultCode isComparable(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        // NULL Check. If one type is NULL, then we return NULL
        if (b1[s1] == ATypeTag.NULL.serialize() || b2[s2] == ATypeTag.NULL.serialize() || b1[s1] == 0 || b1[s2] == 0) {
            return ComparableResultCode.UNKNOWN;
        }

        // Checks whether two types are comparable or not
        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
        ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);

        // Are two types compatible, meaning that they can be compared? (e.g., compare between numeric types
        if (ATypeHierarchy.isCompatible(tag1, tag2)) {
            return ComparableResultCode.TRUE;
        } else {
            return ComparableResultCode.FALSE;
        }

    }

    public abstract int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException;

}
