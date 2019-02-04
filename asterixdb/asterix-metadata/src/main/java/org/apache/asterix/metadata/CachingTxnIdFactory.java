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
package org.apache.asterix.metadata;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.runtime.message.TxnIdBlockRequest;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a factory to generate unique transaction IDs.
 */
class CachingTxnIdFactory implements ITxnIdFactory {
    private static final Logger LOGGER = LogManager.getLogger();

    private final INcApplicationContext appCtx;
    private volatile Block block = new Block(0, 0);

    public CachingTxnIdFactory(INcApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    @Override
    public TxnId create() throws AlgebricksException {
        while (true) {
            try {
                return new TxnId(block.nextId());
            } catch (BlockExhaustedException ex) {
                // retry
                LOGGER.info("block exhausted; obtaining new block from supplier");
                TxnIdBlockRequest.Block newBlock;
                try {
                    newBlock = TxnIdBlockRequest.send(appCtx);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                block = new Block(newBlock.getStartingId(), newBlock.getBlockSize());
            }
        }
    }

    @Override
    public void ensureMinimumId(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdBlock(int blockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxTxnId() {
        return block.endExclusive - 1;
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
            long nextId = id.getAndIncrement();
            if (nextId >= endExclusive && (endExclusive >= start || nextId < start)) {
                throw BLOCK_EXHAUSTED_EXCEPTION;
            }
            return nextId;
        }
    }

    private static class BlockExhaustedException extends Exception {
        private static final long serialVersionUID = 8967868415735213490L;
    }
}
