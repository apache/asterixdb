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

package org.apache.asterix.runtime.utils;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;

public class BulkTxnIdFactory implements ITxnIdFactory {

    private final AtomicLong maxId = new AtomicLong();

    @Override
    public TxnId create() {
        return new TxnId(maxId.incrementAndGet());
    }

    @Override
    public long getIdBlock(int blockSize) {
        if (blockSize < 1) {
            throw new IllegalArgumentException("block size cannot be smaller than 1, but was " + blockSize);
        }
        return maxId.getAndAdd(blockSize) + 1;
    }

    @Override
    public void ensureMinimumId(long id) {
        this.maxId.getAndUpdate(next -> Math.max(next, id));
    }

    @Override
    public long getMaxTxnId() {
        return maxId.get();
    }
}
