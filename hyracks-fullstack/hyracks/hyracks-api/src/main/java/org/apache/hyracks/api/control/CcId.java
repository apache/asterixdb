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
package org.apache.hyracks.api.control;

import java.io.Serializable;

public class CcId implements Serializable {

    private static final long serialVersionUID = 1L;

    private short id;

    private CcId(short id) {
        this.id = id;
    }

    public static CcId valueOf(String ccIdString) {
        return new CcId(Integer.decode(ccIdString).shortValue());
    }

    public static CcId valueOf(int ccId) {
        if ((ccId & ~0xffff) != 0) {
            throw new IllegalArgumentException("ccId cannot exceed 16-bits: " + Integer.toHexString(ccId));
        }
        return new CcId((short) ccId);
    }

    public short shortValue() {
        return id;
    }

    public long toLongMask() {
        return (long) id << CcIdPartitionedLongFactory.ID_BITS;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CcId && id == ((CcId) obj).id;
    }

    @Override
    public String toString() {
        return "CC:" + Integer.toHexString(((int) id) & 0xffff);
    }
}
