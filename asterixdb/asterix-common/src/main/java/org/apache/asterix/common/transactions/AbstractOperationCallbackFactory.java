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

import org.apache.asterix.common.context.ITransactionSubsystemProvider;

public abstract class AbstractOperationCallbackFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int datasetId;
    protected final int[] primaryKeyFields;
    protected final ITransactionSubsystemProvider txnSubsystemProvider;
    protected final byte resourceType;

    public AbstractOperationCallbackFactory(int datasetId, int[] primaryKeyFields,
            ITransactionSubsystemProvider txnSubsystemProvider, byte resourceType) {
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.txnSubsystemProvider = txnSubsystemProvider;
        this.resourceType = resourceType;
    }
}
