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

package org.apache.hyracks.storage.common;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class EnforcedIndexCursor implements IIndexCursor {
    enum State {
        CLOSED,
        OPENED,
        DESTROYED
    }

    private static final boolean STORE_TRACES = false;
    private static final boolean ENFORCE_NEXT_HAS_NEXT = true;
    private static final boolean ENFORCE_OPEN_CLOSE_DESTROY = true;
    private static final Logger LOGGER = LogManager.getLogger();

    private State state = State.CLOSED;
    private StackTraceElement[] openCallStack;
    private StackTraceElement[] destroyCallStack;

    @Override
    public final void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (ENFORCE_OPEN_CLOSE_DESTROY && state != State.CLOSED) {
            if (STORE_TRACES && destroyCallStack != null) {
                LOGGER.log(Level.WARN, "The cursor was destroyed in " + Arrays.toString(destroyCallStack));
            }
            throw new IllegalStateException("Cannot open a cursor in the state " + state);
        }
        doOpen(initialState, searchPred);
        state = State.OPENED;
        if (STORE_TRACES) {
            openCallStack = new Throwable().getStackTrace();
        }
    }

    protected abstract void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred)
            throws HyracksDataException;

    @Override
    public final boolean hasNext() throws HyracksDataException {
        if (ENFORCE_NEXT_HAS_NEXT && state != State.OPENED) {
            throw new IllegalStateException("Cannot call hasNext() on a cursor in the state " + state);
        }
        return doHasNext();
    }

    protected abstract boolean doHasNext() throws HyracksDataException;

    @Override
    public final void next() throws HyracksDataException {
        if (ENFORCE_NEXT_HAS_NEXT && state != State.OPENED) {
            throw new IllegalStateException("Cannot call next() on a cursor in the state " + state);
        }
        doNext();
    }

    protected abstract void doNext() throws HyracksDataException;

    @Override
    public final void destroy() throws HyracksDataException {
        if (ENFORCE_OPEN_CLOSE_DESTROY) {
            if (state == State.DESTROYED) {
                return;
            } else if (state != State.CLOSED) {
                if (STORE_TRACES && openCallStack != null) {
                    LOGGER.log(Level.WARN, "The cursor was opened in " + Arrays.toString(openCallStack));
                }
                throw new IllegalStateException("Cannot destroy a cursor in the state " + state);
            }
        }
        state = State.DESTROYED;
        try {
            doDestroy();
        } finally {
            if (ENFORCE_OPEN_CLOSE_DESTROY && STORE_TRACES) {
                destroyCallStack = new Throwable().getStackTrace();
            }
        }
    }

    protected abstract void doDestroy() throws HyracksDataException;

    @Override
    public final void close() throws HyracksDataException {
        if (ENFORCE_OPEN_CLOSE_DESTROY) {
            if (state == State.CLOSED) {
                return;
            } else if (state == State.DESTROYED) {
                throw new IllegalStateException("Cannot close a cursor in the state " + state);
            }
        }
        state = State.CLOSED;
        doClose();
    }

    protected abstract void doClose() throws HyracksDataException;

    @Override
    public final ITupleReference getTuple() {
        if (state == State.OPENED) {
            return doGetTuple();
        } else {
            return null;
        }
    }

    protected abstract ITupleReference doGetTuple();
}
