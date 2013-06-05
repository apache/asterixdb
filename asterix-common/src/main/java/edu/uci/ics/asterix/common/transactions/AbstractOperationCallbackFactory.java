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

package edu.uci.ics.asterix.common.transactions;

import java.io.Serializable;

import edu.uci.ics.asterix.common.context.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.common.transactions.JobId;

public abstract class AbstractOperationCallbackFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final JobId jobId;
    protected final int datasetId;
    protected final int[] primaryKeyFields;
    protected final ITransactionSubsystemProvider txnSubsystemProvider;
    protected final byte resourceType;

    public AbstractOperationCallbackFactory(JobId jobId, int datasetId, int[] primaryKeyFields,
            ITransactionSubsystemProvider txnSubsystemProvider, byte resourceType) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.txnSubsystemProvider = txnSubsystemProvider;
        this.resourceType = resourceType;
    }
}
