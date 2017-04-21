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

package org.apache.asterix.common.context;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * The purpose of this provider is to work around a cyclic dependency between asterix-common and asterix-transactions.
 * The operation callbacks would depend on the AsterixAppRuntimeContext to get the transaction subsystem,
 * while at the same time the AsterixAppRuntimeContext depends on asterix-transactions for the TransactionSubsystem.
 */
public class TransactionSubsystemProvider implements ITransactionSubsystemProvider {
    private static final long serialVersionUID = 1L;
    public static final TransactionSubsystemProvider INSTANCE = new TransactionSubsystemProvider();

    private TransactionSubsystemProvider() {
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem(IHyracksTaskContext ctx) {
        INcApplicationContext appCtx =
                (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
        return appCtx.getTransactionSubsystem();
    }
}
