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

import org.apache.hyracks.api.dataflow.IDestroyable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Represents an index cursor. The expected use
 * cursor = new cursor();
 * try{
 * -while (more predicates){
 * --cursor.open(predicate);
 * --try{
 * ---while (cursor.hasNext()){
 * ----cursor.next()
 * ---}
 * --} finally{
 * ---cursor.close();
 * --}
 * -}
 * } finally{
 * -cursor.destroy();
 * }
 * Each created cursor must have destroy called
 * Each successfully opened cursor must have close called
 *
 * A cursor is a state machine that works as follows:
 * The states are:
 * <ul>
 * <li>CLOSED</li>
 * <li>OPENED</li>
 * <li>DESTROYED</li>
 * </ul>
 * When a cursor object is created, it is in the CLOSED state.
 * CLOSED: Legal calls are open() --> OPENED, or destroy() --> DESTROYED, close() --> no effect
 * OPENED: The only legal calls are hasNext(), next(), or close() --> CLOSED.
 * DESTROYED: The only legal call is destroy() which has no effect.
 *
 * Cursors must enforce the cursor state machine
 */
public interface IIndexCursor extends IDestroyable {
    /**
     * Opens the cursor
     * if open succeeds, close must be called.
     *
     * @param initialState
     * @param searchPred
     * @throws HyracksDataException
     */
    void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException;

    /**
     * True if the cursor has a next value
     *
     * @return
     * @throws HyracksDataException
     */
    boolean hasNext() throws HyracksDataException;

    /**
     * Moves the cursor to the next value
     *
     * @throws HyracksDataException
     */
    void next() throws HyracksDataException;

    /**
     * Close the cursor. If the cursor is already closed then invoking this
     * method has no effect.
     *
     * @throws HyracksDataException
     */
    void close() throws HyracksDataException;

    /**
     * @return the tuple pointed to by the cursor
     */
    ITupleReference getTuple();
}
