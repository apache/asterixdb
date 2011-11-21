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

package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class BinaryTokenizerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final IBinaryTokenizerFactory tokenizerFactory;
    // Fields that will be tokenized
    private final int[] tokenFields;
    // operator will append these key fields to each token, e.g., as
    // payload for an inverted list
    // WARNING: too many key fields can cause significant data blowup.
    private final int[] keyFields;

    public BinaryTokenizerOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IBinaryTokenizerFactory tokenizerFactory, int[] tokenFields, int[] keyFields) {
        super(spec, 1, 1);
        this.tokenizerFactory = tokenizerFactory;
        this.tokenFields = tokenFields;
        this.keyFields = keyFields;
        recordDescriptors[0] = recDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions) throws HyracksDataException {
        return new BinaryTokenizerOperatorNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(odId, 0),
                recordDescriptors[0], tokenizerFactory.createTokenizer(), tokenFields, keyFields);
    }
}
