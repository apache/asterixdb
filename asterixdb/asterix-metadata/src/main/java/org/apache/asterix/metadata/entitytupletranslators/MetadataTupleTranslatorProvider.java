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
import org.apache.asterix.metadata.bootstrap.MetadataIndexesProvider;

public class MetadataTupleTranslatorProvider {

    protected final MetadataIndexesProvider mdIndexesProvider;

    public MetadataTupleTranslatorProvider(MetadataIndexesProvider metadataIndexesProvider) {
        this.mdIndexesProvider = metadataIndexesProvider;
    }

    public CompactionPolicyTupleTranslator getCompactionPolicyTupleTranslator(boolean getTuple) {
        return new CompactionPolicyTupleTranslator(getTuple, mdIndexesProvider.getCompactionPolicyEntity());
    }

    public DatasetTupleTranslator getDatasetTupleTranslator(boolean getTuple) {
        return new DatasetTupleTranslator(getTuple, mdIndexesProvider.getDatasetEntity());
    }

    public DatasourceAdapterTupleTranslator getAdapterTupleTranslator(boolean getTuple) {
        return new DatasourceAdapterTupleTranslator(getTuple, mdIndexesProvider.getDatasourceAdapterEntity());
    }

    public DatatypeTupleTranslator getDataTypeTupleTranslator(TxnId txnId, MetadataNode metadataNode,
            boolean getTuple) {
        return new DatatypeTupleTranslator(txnId, metadataNode, getTuple, mdIndexesProvider.getDatatypeEntity());
    }

    public DatabaseTupleTranslator getDatabaseTupleTranslator(boolean getTuple) {
        return new DatabaseTupleTranslator(getTuple, mdIndexesProvider.getDatabaseEntity());
    }

    public DataverseTupleTranslator getDataverseTupleTranslator(boolean getTuple) {
        return new DataverseTupleTranslator(getTuple, mdIndexesProvider.getDataverseEntity());
    }

    public ExternalFileTupleTranslator getExternalFileTupleTranslator(boolean getTuple) {
        return new ExternalFileTupleTranslator(getTuple, mdIndexesProvider.getExternalFileEntity());
    }

    public FeedPolicyTupleTranslator getFeedPolicyTupleTranslator(boolean getTuple) {
        return new FeedPolicyTupleTranslator(getTuple, mdIndexesProvider.getFeedPolicyEntity());
    }

    public FeedTupleTranslator getFeedTupleTranslator(boolean getTuple) {
        return new FeedTupleTranslator(getTuple, mdIndexesProvider.getFeedEntity());
    }

    public FeedConnectionTupleTranslator getFeedConnectionTupleTranslator(boolean getTuple) {
        return new FeedConnectionTupleTranslator(getTuple, mdIndexesProvider.getFeedConnectionEntity());
    }

    public FunctionTupleTranslator getFunctionTupleTranslator(TxnId txnId, MetadataNode metadataNode,
            boolean getTuple) {
        return new FunctionTupleTranslator(txnId, metadataNode, getTuple, mdIndexesProvider.getFunctionEntity());
    }

    public FullTextConfigMetadataEntityTupleTranslator getFullTextConfigTupleTranslator(boolean getTuple) {
        return new FullTextConfigMetadataEntityTupleTranslator(getTuple, mdIndexesProvider.getFullTextConfigEntity());
    }

    public FullTextFilterMetadataEntityTupleTranslator getFullTextFilterTupleTranslator(boolean getTuple) {
        return new FullTextFilterMetadataEntityTupleTranslator(getTuple, mdIndexesProvider.getFullTextFilterEntity());
    }

    public IndexTupleTranslator getIndexTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        return new IndexTupleTranslator(txnId, metadataNode, getTuple, mdIndexesProvider.getIndexEntity());
    }

    public LibraryTupleTranslator getLibraryTupleTranslator(boolean getTuple) {
        return new LibraryTupleTranslator(getTuple, mdIndexesProvider.getLibraryEntity());
    }

    public NodeTupleTranslator getNodeTupleTranslator(boolean getTuple) {
        return new NodeTupleTranslator(getTuple, mdIndexesProvider.getNodeEntity());
    }

    public NodeGroupTupleTranslator getNodeGroupTupleTranslator(boolean getTuple) {
        return new NodeGroupTupleTranslator(getTuple, mdIndexesProvider.getNodeGroupEntity());
    }

    public SynonymTupleTranslator getSynonymTupleTranslator(boolean getTuple) {
        return new SynonymTupleTranslator(getTuple, mdIndexesProvider.getSynonymEntity());
    }
}
