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
package org.apache.asterix.metadata.entitytupletranslators;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;

public class MetadataTupleTranslatorProvider {

    public CompactionPolicyTupleTranslator getCompactionPolicyTupleTranslator(boolean getTuple) {
        return new CompactionPolicyTupleTranslator(getTuple);
    }

    public DatasetTupleTranslator getDatasetTupleTranslator(boolean getTuple) {
        return new DatasetTupleTranslator(getTuple);
    }

    public DatasourceAdapterTupleTranslator getAdapterTupleTranslator(boolean getTuple) {
        return new DatasourceAdapterTupleTranslator(getTuple);
    }

    public DatatypeTupleTranslator getDataTypeTupleTranslator(TxnId txnId, MetadataNode metadataNode,
            boolean getTuple) {
        return new DatatypeTupleTranslator(txnId, metadataNode, getTuple);
    }

    public DataverseTupleTranslator getDataverseTupleTranslator(boolean getTuple) {
        return new DataverseTupleTranslator(getTuple);
    }

    public ExternalFileTupleTranslator getExternalFileTupleTranslator(boolean getTuple) {
        return new ExternalFileTupleTranslator(getTuple);
    }

    public FeedPolicyTupleTranslator getFeedPolicyTupleTranslator(boolean getTuple) {
        return new FeedPolicyTupleTranslator(getTuple);
    }

    public FeedTupleTranslator getFeedTupleTranslator(boolean getTuple) {
        return new FeedTupleTranslator(getTuple);
    }

    public FunctionTupleTranslator getFunctionTupleTranslator(boolean getTuple) {
        return new FunctionTupleTranslator(getTuple);
    }

    public IndexTupleTranslator getIndexTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        return new IndexTupleTranslator(txnId, metadataNode, getTuple);
    }

    public LibraryTupleTranslator getLibraryTupleTranslator(boolean getTuple) {
        return new LibraryTupleTranslator(getTuple);
    }

    public NodeTupleTranslator getNodeTupleTranslator(boolean getTuple) {
        return new NodeTupleTranslator(getTuple);
    }

    public NodeGroupTupleTranslator getNodeGroupTupleTranslator(boolean getTuple) {
        return new NodeGroupTupleTranslator(getTuple);
    }
}
