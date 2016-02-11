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
package org.apache.asterix.translator;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement.Kind;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * An AQL statement instance is translated into an instance of type CompileX
 * that has additional fields for use by the AqlTranslator.
 */
public class CompiledStatements {

    public static interface ICompiledStatement {

        public Kind getKind();
    }

    public static class CompiledDatasetDropStatement implements ICompiledStatement {
        private final String dataverseName;
        private final String datasetName;

        public CompiledDatasetDropStatement(String dataverseName, String datasetName) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public Kind getKind() {
            return Kind.DATASET_DROP;
        }
    }

    // added by yasser
    public static class CompiledCreateDataverseStatement implements ICompiledStatement {
        private String dataverseName;
        private String format;

        public CompiledCreateDataverseStatement(String dataverseName, String format) {
            this.dataverseName = dataverseName;
            this.format = format;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public Kind getKind() {
            return Kind.CREATE_DATAVERSE;
        }
    }

    public static class CompiledNodeGroupDropStatement implements ICompiledStatement {
        private String nodeGroupName;

        public CompiledNodeGroupDropStatement(String nodeGroupName) {
            this.nodeGroupName = nodeGroupName;
        }

        public String getNodeGroupName() {
            return nodeGroupName;
        }

        @Override
        public Kind getKind() {
            return Kind.NODEGROUP_DROP;
        }
    }

    public static class CompiledIndexDropStatement implements ICompiledStatement {
        private String dataverseName;
        private String datasetName;
        private String indexName;

        public CompiledIndexDropStatement(String dataverseName, String datasetName, String indexName) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.indexName = indexName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getIndexName() {
            return indexName;
        }

        @Override
        public Kind getKind() {
            return Kind.INDEX_DROP;
        }
    }

    public static class CompiledDataverseDropStatement implements ICompiledStatement {
        private String dataverseName;
        private boolean ifExists;

        public CompiledDataverseDropStatement(String dataverseName, boolean ifExists) {
            this.dataverseName = dataverseName;
            this.ifExists = ifExists;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public boolean getIfExists() {
            return ifExists;
        }

        @Override
        public Kind getKind() {
            return Kind.DATAVERSE_DROP;
        }
    }

    public static class CompiledTypeDropStatement implements ICompiledStatement {
        private String typeName;

        public CompiledTypeDropStatement(String nodeGroupName) {
            this.typeName = nodeGroupName;
        }

        public String getTypeName() {
            return typeName;
        }

        @Override
        public Kind getKind() {
            return Kind.TYPE_DROP;
        }
    }

    public static interface ICompiledDmlStatement extends ICompiledStatement {

        public String getDataverseName();

        public String getDatasetName();
    }

    public static class CompiledCreateIndexStatement implements ICompiledDmlStatement {
        private final String indexName;
        private final String dataverseName;
        private final String datasetName;
        private final List<List<String>> keyFields;
        private final List<IAType> keyTypes;
        private final boolean isEnforced;
        private final IndexType indexType;

        // Specific to NGram index.
        private final int gramLength;

        public CompiledCreateIndexStatement(String indexName, String dataverseName, String datasetName,
                List<List<String>> keyFields, List<IAType> keyTypes, boolean isEnforced, int gramLength,
                IndexType indexType) {
            this.indexName = indexName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.keyFields = keyFields;
            this.keyTypes = keyTypes;
            this.gramLength = gramLength;
            this.isEnforced = isEnforced;
            this.indexType = indexType;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<List<String>> getKeyFields() {
            return keyFields;
        }

        public List<IAType> getKeyFieldTypes() {
            return keyTypes;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public int getGramLength() {
            return gramLength;
        }

        public boolean isEnforced() {
            return isEnforced;
        }

        @Override
        public Kind getKind() {
            return Kind.CREATE_INDEX;
        }
    }

    public static class CompiledLoadFromFileStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private boolean alreadySorted;
        private String adapter;
        private Map<String, String> properties;

        public CompiledLoadFromFileStatement(String dataverseName, String datasetName, String adapter,
                Map<String, String> properties, boolean alreadySorted) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.alreadySorted = alreadySorted;
            this.adapter = adapter;
            this.properties = properties;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public boolean alreadySorted() {
            return alreadySorted;
        }

        public String getAdapter() {
            return adapter;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public Kind getKind() {
            return Kind.LOAD;
        }
    }

    public static class CompiledInsertStatement implements ICompiledDmlStatement {
        private final String dataverseName;
        private final String datasetName;
        private final Query query;
        private final int varCounter;

        public CompiledInsertStatement(String dataverseName, String datasetName, Query query, int varCounter) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.INSERT;
        }
    }

    public static class CompiledUpsertStatement extends CompiledInsertStatement {

        public CompiledUpsertStatement(String dataverseName, String datasetName, Query query, int varCounter) {
            super(dataverseName, datasetName, query, varCounter);
        }

        @Override
        public Kind getKind() {
            return Kind.UPSERT;
        }
    }

    public static class CompiledConnectFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String feedName;
        private String datasetName;
        private String policyName;
        private Query query;
        private int varCounter;

        public CompiledConnectFeedStatement(String dataverseName, String feedName, String datasetName,
                String policyName, Query query, int varCounter) {
            this.dataverseName = dataverseName;
            this.feedName = feedName;
            this.datasetName = datasetName;
            this.policyName = policyName;
            this.query = query;
            this.varCounter = varCounter;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        public String getFeedName() {
            return feedName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        public void setQuery(Query query) {
            this.query = query;
        }

        @Override
        public Kind getKind() {
            return Kind.CONNECT_FEED;
        }

        public String getPolicyName() {
            return policyName;
        }
    }

    public static class CompiledSubscribeFeedStatement implements ICompiledDmlStatement {

        private final FeedConnectionRequest request;
        private Query query;
        private final int varCounter;

        public CompiledSubscribeFeedStatement(FeedConnectionRequest request, Query query, int varCounter) {
            this.request = request;
            this.query = query;
            this.varCounter = varCounter;
        }

        @Override
        public String getDataverseName() {
            return request.getReceivingFeedId().getDataverse();
        }

        @Override
        public String getDatasetName() {
            return request.getTargetDataset();
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        public void setQuery(Query query) {
            this.query = query;
        }

        @Override
        public Kind getKind() {
            return Kind.SUBSCRIBE_FEED;
        }

    }

    public static class CompiledDisconnectFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private String feedName;
        private Query query;
        private int varCounter;

        public CompiledDisconnectFeedStatement(String dataverseName, String feedName, String datasetName) {
            this.dataverseName = dataverseName;
            this.feedName = feedName;
            this.datasetName = datasetName;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public String getFeedName() {
            return feedName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.DISCONNECT_FEED;
        }

    }

    public static class CompiledDeleteStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private Expression condition;
        private int varCounter;
        private Query query;

        public CompiledDeleteStatement(VariableExpr var, String dataverseName, String datasetName, Expression condition,
                int varCounter, Query query) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.condition = condition;
            this.varCounter = varCounter;
            this.query = query;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Expression getCondition() {
            return condition;
        }

        public Query getQuery() throws AlgebricksException {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.DELETE;
        }

    }

    public static class CompiledCompactStatement implements ICompiledStatement {
        private final String dataverseName;
        private final String datasetName;

        public CompiledCompactStatement(String dataverseName, String datasetName) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public Kind getKind() {
            return Kind.COMPACT;
        }
    }

    public static class CompiledIndexCompactStatement extends CompiledCompactStatement {
        private final String indexName;
        private final List<List<String>> keyFields;
        private final List<IAType> keyTypes;
        private final IndexType indexType;
        private final boolean isEnforced;

        // Specific to NGram index.
        private final int gramLength;

        public CompiledIndexCompactStatement(String dataverseName, String datasetName, String indexName,
                List<List<String>> keyFields, List<IAType> keyTypes, boolean isEnforced, int gramLength,
                IndexType indexType) {
            super(dataverseName, datasetName);
            this.indexName = indexName;
            this.keyFields = keyFields;
            this.keyTypes = keyTypes;
            this.gramLength = gramLength;
            this.indexType = indexType;
            this.isEnforced = isEnforced;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<List<String>> getKeyFields() {
            return keyFields;
        }

        public List<IAType> getKeyTypes() {
            return keyTypes;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public int getGramLength() {
            return gramLength;
        }

        public boolean isEnforced() {
            return isEnforced;
        }
    }
}
