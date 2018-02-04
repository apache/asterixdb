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
import java.util.function.Supplier;

import org.apache.asterix.common.transactions.ILongBlockFactory;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a factory to generate unique transaction IDs.
 */
class CcTxnIdFactory implements ITxnIdFactory {
    private static final int TXN_BLOCK_SIZE = 1024;
    private static final Logger LOGGER = LogManager.getLogger();

    private final Supplier<ILongBlockFactory> blockFactorySupplier;
    private volatile Block block = new Block(0, 0);

    public CcTxnIdFactory(Supplier<ILongBlockFactory> blockFactorySupplier) {
        this.blockFactorySupplier = blockFactorySupplier;
    }

    @Override
    public TxnId create() throws AlgebricksException {
        while (true) {
            try {
                return new TxnId(block.nextId());
            } catch (BlockExhaustedException ex) {
                // retry
                LOGGER.info("block exhausted; obtaining new block from supplier");
                block = new Block(blockFactorySupplier.get().getBlock(TXN_BLOCK_SIZE), TXN_BLOCK_SIZE);
            }
        }
    }

    @Override
    public void ensureMinimumId(long id) throws AlgebricksException {
        blockFactorySupplier.get().ensureMinimum(id);
    }

    static class Block {
        private static final BlockExhaustedException BLOCK_EXHAUSTED_EXCEPTION = new BlockExhaustedException();
        private final AtomicLong id;
        private final long start;
        private final long endExclusive;

        private Block(long start, long blockSize) {
            this.id = new AtomicLong(start);
            this.start = start;
            this.endExclusive = start + blockSize;
        }

        private long nextId() throws BlockExhaustedException {
            long nextId = id.incrementAndGet();
            if (nextId >= endExclusive && (endExclusive >= start || nextId < start)) {
                throw BLOCK_EXHAUSTED_EXCEPTION;
            }
            return nextId;
        }
    }

    private static class BlockExhaustedException extends Exception {
    }
}
