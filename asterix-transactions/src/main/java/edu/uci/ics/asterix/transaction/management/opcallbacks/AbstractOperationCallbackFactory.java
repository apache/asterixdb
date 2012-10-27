/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;

public abstract class AbstractOperationCallbackFactory {
    protected final JobId jobId;
    protected final DatasetId datasetId;
    protected final int[] primaryKeyFields;
    protected final IBinaryHashFunction[] primaryKeyHashFunctions;
    protected final ITransactionSubsystemProvider txnSubsystemProvider;

    public AbstractOperationCallbackFactory(JobId jobId, DatasetId datasetId, int[] primaryKeyFields,
            IBinaryHashFunction[] primaryKeyHashFunctions, ITransactionSubsystemProvider txnSubsystemProvider) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.primaryKeyHashFunctions = primaryKeyHashFunctions;
        this.txnSubsystemProvider = txnSubsystemProvider;
    }
}
