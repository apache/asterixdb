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
package org.apache.hyracks.storage.am.btree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IBTreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;

public class BTreeTypeAwareTupleReference extends TypeAwareTupleReference implements IBTreeIndexTupleReference {

    public static final byte UPDATE_BIT_OFFSET = 6;

    protected final boolean updateAware;

    public BTreeTypeAwareTupleReference(ITypeTraits[] typeTraits, boolean updateAware) {
        super(typeTraits);
        this.updateAware = updateAware;
    }

    @Override
    public boolean flipUpdated() {
        if (updateAware) {
            final byte mask = (byte) (1 << UPDATE_BIT_OFFSET);
            if ((buf[tupleStartOff] & mask) == 0) {
                buf[tupleStartOff] |= mask;
                return true;
            } else {
                buf[tupleStartOff] &= ~mask;
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean isUpdated() {
        if (updateAware) {
            return BitOperationUtils.getBit(buf, tupleStartOff, UPDATE_BIT_OFFSET);
        } else {
            return false;
        }
    }

}
