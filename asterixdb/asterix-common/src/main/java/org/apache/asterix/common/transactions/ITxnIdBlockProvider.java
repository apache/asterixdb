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

package org.apache.asterix.common.transactions;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ITxnIdBlockProvider extends Remote, Serializable {

    /**
     * Ensures that future transaction blocks will be of a value larger than the supplied value
     *
     * @param maxId
     *            The txn id to ensure future txn ids are larger than
     * @throws RemoteException
     */
    void ensureMinimumTxnId(long maxId) throws RemoteException;

    /**
     * Allocates a block of transaction ids of specified block size
     *
     * @param blockSize
     *            The size of the transaction id block to reserve
     * @return the start of the reserved block
     * @throws RemoteException
     */
    long reserveTxnIdBlock(int blockSize) throws RemoteException;

}
