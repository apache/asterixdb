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

package org.apache.asterix.metadata.api;

import java.io.IOException;
import java.rmi.RemoteException;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts an object of type T from a given tuple. Implementations of this
 * interface are used for extracting specific fields of metadata records within
 * the metadata node. The intention is to have a single transactional search
 * procedure than can deliver different objects based on the type of value
 * extractor passed to the search procedure.
 */
public interface IValueExtractor<T> {
    /**
     * Extracts an object of type T from a given tuple.
     *
     * @param txnId
     *            A globally unique transaction id.
     * @param tuple
     *            Tuple from which an object shall be extracted.
     * @return New object of type T.
     * @throws AlgebricksException
     * @throws HyracksDataException
     * @throws IOException
     */
    T getValue(TxnId txnId, ITupleReference tuple) throws AlgebricksException, HyracksDataException, RemoteException;
}
