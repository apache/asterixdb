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

import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class BinaryTokenizerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final IBinaryTokenizerFactory tokenizerFactory;
    // fields that will be tokenized
    private final int[] tokenFields;
    // operator will emit these projected fields for each token, e.g., as
    // payload for an inverted list
    // WARNING: too many projected fields can cause significant data blowup
    private final int[] projFields;

    public BinaryTokenizerOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IBinaryTokenizerFactory tokenizerFactory, int[] tokenFields, int[] projFields) {
        super(spec, 1, 1);
        this.tokenizerFactory = tokenizerFactory;
        this.tokenFields = tokenFields;
        this.projFields = projFields;
        recordDescriptors[0] = recDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new BinaryTokenizerOperatorNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(odId, 0),
                recordDescriptors[0], tokenizerFactory.createTokenizer(), tokenFields, projFields);
    }
}
