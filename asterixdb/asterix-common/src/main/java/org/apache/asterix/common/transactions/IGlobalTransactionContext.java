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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;

public interface IGlobalTransactionContext {

    JobId getJobId();

    int incrementAndGetAcksReceived();

    int getAcksReceived();

    int getNumNodes();

    int getNumPartitions();

    void resetAcksReceived();

    void setTxnStatus(IGlobalTxManager.TransactionStatus status);

    IGlobalTxManager.TransactionStatus getTxnStatus();

    List<Integer> getDatasetIds();

    Map<String, Map<String, ILSMComponentId>> getNodeResourceMap();

    void persist(IOManager ioManager);

    void delete(IOManager ioManager);

}
