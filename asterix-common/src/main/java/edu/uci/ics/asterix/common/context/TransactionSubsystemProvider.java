/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * The purpose of this provider is to work around a cyclic dependency between asterix-common and asterix-transactions.
 * The operation callbacks would depend on the AsterixAppRuntimeContext to get the transaction subsystem,
 * while at the same time the AsterixAppRuntimeContext depends on asterix-transactions for the TransactionSubsystem.
 */
public class TransactionSubsystemProvider implements ITransactionSubsystemProvider {
    @Override
    public ITransactionSubsystem getTransactionSubsystem(IHyracksTaskContext ctx) {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        return runtimeCtx.getTransactionSubsystem();
    }
}
