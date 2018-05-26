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

import java.util.Map;

import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * An AQL statement instance is translated into an instance of type CompileX
 * that has additional fields for use by the AqlTranslator.
 */
public class CompiledStatements {

    public interface ICompiledStatement {
        Statement.Kind getKind();

        SourceLocation getSourceLocation();
    }

    public static abstract class AbstractCompiledStatement implements ICompiledStatement {
        private SourceLocation sourceLoc;

        public void setSourceLocation(SourceLocation sourceLoc) {
            this.sourceLoc = sourceLoc;
        }

        public SourceLocation getSourceLocation() {
            return sourceLoc;
        }
    }

    public static class CompiledDatasetDropStatement extends AbstractCompiledStatement {
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
        public Statement.Kind getKind() {
            return Statement.Kind.DATASET_DROP;
        }
    }

    // added by yasser
    public static class CompiledCreateDataverseStatement extends AbstractCompiledStatement {
        private final String dataverseName;
        private final String format;

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
        public Statement.Kind getKind() {
            return Statement.Kind.CREATE_DATAVERSE;
        }
    }

    public static class CompiledNodeGroupDropStatement extends AbstractCompiledStatement {
        private final String nodeGroupName;

        public CompiledNodeGroupDropStatement(String nodeGroupName) {
            this.nodeGroupName = nodeGroupName;
        }

        public String getNodeGroupName() {
            return nodeGroupName;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.NODEGROUP_DROP;
        }
    }

    public static class CompiledIndexDropStatement extends AbstractCompiledStatement {
        private final String dataverseName;
        private final String datasetName;
        private final String indexName;

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
        public Statement.Kind getKind() {
            return Statement.Kind.INDEX_DROP;
        }
    }

    public static class CompiledDataverseDropStatement extends AbstractCompiledStatement {
        private final String dataverseName;
        private final boolean ifExists;

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
        public Statement.Kind getKind() {
            return Statement.Kind.DATAVERSE_DROP;
        }
    }

    public static class CompiledTypeDropStatement extends AbstractCompiledStatement {
        private final String typeName;

        public CompiledTypeDropStatement(String nodeGroupName) {
            this.typeName = nodeGroupName;
        }

        public String getTypeName() {
            return typeName;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.TYPE_DROP;
        }
    }

    public interface ICompiledDmlStatement extends ICompiledStatement {

        String getDataverseName();

        String getDatasetName();
    }

    public static class CompiledCreateIndexStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {
        private final Dataset dataset;
        private final Index index;

        public CompiledCreateIndexStatement(Dataset dataset, Index index) {
            this.dataset = dataset;
            this.index = index;
        }

        @Override
        public String getDatasetName() {
            return index.getDatasetName();
        }

        @Override
        public String getDataverseName() {
            return index.getDataverseName();
        }

        public Index getIndex() {
            return index;
        }

        public Dataset getDataset() {
            return dataset;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.CREATE_INDEX;
        }
    }

    public static class CompiledLoadFromFileStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {
        private final String dataverseName;
        private final String datasetName;
        private final boolean alreadySorted;
        private final String adapter;
        private final Map<String, String> properties;

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
        public Statement.Kind getKind() {
            return Statement.Kind.LOAD;
        }
    }

    public static class CompiledInsertStatement extends AbstractCompiledStatement implements ICompiledDmlStatement {
        private final String dataverseName;
        private final String datasetName;
        private final Query query;
        private final int varCounter;
        private final VariableExpr var;
        private final Expression returnExpression;

        public CompiledInsertStatement(String dataverseName, String datasetName, Query query, int varCounter,
                VariableExpr var, Expression returnExpression) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
            this.var = var;
            this.returnExpression = returnExpression;
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

        public VariableExpr getVar() {
            return var;
        }

        public Expression getReturnExpression() {
            return returnExpression;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.INSERT;
        }
    }

    public static class CompiledUpsertStatement extends CompiledInsertStatement {

        public CompiledUpsertStatement(String dataverseName, String datasetName, Query query, int varCounter,
                VariableExpr var, Expression returnExpression) {
            super(dataverseName, datasetName, query, varCounter, var, returnExpression);
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.UPSERT;
        }
    }

    public static class CompiledSubscribeFeedStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {

        private FeedConnectionRequest request;
        private final int varCounter;

        public CompiledSubscribeFeedStatement(FeedConnectionRequest request, int varCounter) {
            this.request = request;
            this.varCounter = varCounter;
        }

        @Override
        public String getDataverseName() {
            return request.getReceivingFeedId().getDataverse();
        }

        public String getFeedName() {
            return request.getReceivingFeedId().getEntityName();
        }

        @Override
        public String getDatasetName() {
            return request.getTargetDataset();
        }

        public int getVarCounter() {
            return varCounter;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.SUBSCRIBE_FEED;
        }
    }

    public static class CompiledDeleteStatement extends AbstractCompiledStatement implements ICompiledDmlStatement {
        private final String dataverseName;
        private final String datasetName;
        private final Expression condition;
        private final int varCounter;
        private final Query query;

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
        public Statement.Kind getKind() {
            return Statement.Kind.DELETE;
        }

    }

    public static class CompiledCompactStatement extends AbstractCompiledStatement {
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
        public Statement.Kind getKind() {
            return Statement.Kind.COMPACT;
        }
    }

    public static class CompiledIndexCompactStatement extends CompiledCompactStatement {
        private Dataset dataset;
        private Index index;

        public CompiledIndexCompactStatement(Dataset dataset, Index index) {
            super(dataset.getDataverseName(), dataset.getDatasetName());
            this.dataset = dataset;
            this.index = index;
        }

        public Dataset getDataset() {
            return dataset;
        }

        public Index getIndex() {
            return index;
        }
    }
}
