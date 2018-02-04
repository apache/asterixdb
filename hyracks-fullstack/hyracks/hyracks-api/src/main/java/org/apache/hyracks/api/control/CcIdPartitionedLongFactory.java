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

import java.util.concurrent.atomic.AtomicLong;

public class CcIdPartitionedLongFactory {
    private static final int CC_BITS = Short.SIZE;
    public static final int ID_BITS = Long.SIZE - CC_BITS;
    public static final long MAX_ID = (1L << ID_BITS) - 1;
    private final CcId ccId;
    private final AtomicLong id;

    public CcIdPartitionedLongFactory(CcId ccId) {
        this.ccId = ccId;
        id = new AtomicLong(ccId.toLongMask());
    }

    protected long nextId() {
        return id.getAndUpdate(prev -> {
            if ((prev & MAX_ID) == MAX_ID) {
                return prev ^ MAX_ID;
            } else {
                return prev + 1;
            }
        });
    }

    protected long maxId() {
        long next = id.get();
        if ((next & MAX_ID) == 0) {
            return next | MAX_ID;
        } else {
            return next - 1;
        }
    }

    protected void ensureMinimumId(long id) {
        if ((id & ~MAX_ID) != ccId.toLongMask()) {
            throw new IllegalArgumentException("cannot change ccId as part of ensureMinimumId() (was: "
                    + Long.toHexString(this.id.get()) + ", given: " + Long.toHexString(id));
        }
        this.id.updateAndGet(current -> Math.max(current, id));
    }

    public CcId getCcId() {
        return ccId;
    }
}
