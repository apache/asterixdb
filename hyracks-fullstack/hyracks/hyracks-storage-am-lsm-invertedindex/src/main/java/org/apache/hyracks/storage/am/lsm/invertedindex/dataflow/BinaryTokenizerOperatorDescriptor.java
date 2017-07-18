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

package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class BinaryTokenizerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final IBinaryTokenizerFactory tokenizerFactory;
    // Field that will be tokenized.
    private final int docField;
    // operator will append these key fields to each token, e.g., as
    // payload for an inverted list
    // WARNING: too many key fields can cause significant data blowup.
    private final int[] keyFields;
    // Indicates whether the first key field should be the number of tokens in the tokenized set of the document.
    // This value is used in partitioned inverted indexes, for example.
    private final boolean addNumTokensKey;
    // Indicates the order of field write
    // True: [keyfield1, ... n , token, number of token (if a partitioned index)]
    // False: [token, number of token(if a partitioned index), keyfield1, keyfield2 ...]
    private final boolean writeKeyFieldsFirst;

    private final boolean writeMissing;

    private final IMissingWriterFactory missingWriterFactory;

    public BinaryTokenizerOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IBinaryTokenizerFactory tokenizerFactory, int docField, int[] keyFields, boolean addNumTokensKey,
            boolean writeKeyFieldsFirst, boolean writeMissing, IMissingWriterFactory missingWriterFactory) {
        super(spec, 1, 1);
        this.tokenizerFactory = tokenizerFactory;
        this.docField = docField;
        this.keyFields = keyFields;
        this.addNumTokensKey = addNumTokensKey;
        outRecDescs[0] = recDesc;
        this.writeKeyFieldsFirst = writeKeyFieldsFirst;
        this.writeMissing = writeMissing;
        this.missingWriterFactory = missingWriterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new BinaryTokenizerOperatorNodePushable(ctx,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), outRecDescs[0],
                tokenizerFactory.createTokenizer(), docField, keyFields, addNumTokensKey, writeKeyFieldsFirst,
                writeMissing, missingWriterFactory);
    }
}
