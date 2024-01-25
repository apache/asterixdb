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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.CopyToStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
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

        String getDatabaseName();

        DataverseName getDataverseName();

        String getDatasetName();

        byte getCategory();
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
        public DataverseName getDataverseName() {
            return index.getDataverseName();
        }

        @Override
        public String getDatabaseName() {
            return index.getDatabaseName();
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

        @Override
        public byte getCategory() {
            return Statement.Category.DDL;
        }
    }

    public static class CompiledLoadFromFileStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {

        private final String databaseName;
        private final DataverseName dataverseName;
        private final String datasetName;
        private final boolean alreadySorted;
        private final String adapter;
        private final Map<String, String> properties;

        public CompiledLoadFromFileStatement(String databaseName, DataverseName dataverseName, String datasetName,
                String adapter, Map<String, String> properties, boolean alreadySorted) {
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.alreadySorted = alreadySorted;
            this.adapter = adapter;
            this.properties = properties;
        }

        @Override
        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public DataverseName getDataverseName() {
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

        @Override
        public byte getCategory() {
            return Statement.Category.UPDATE;
        }
    }

    public static class CompiledCopyFromFileStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {

        private final String databaseName;
        private final DataverseName dataverseName;
        private final String datasetName;
        private final Datatype itemType;
        private final String adapter;
        private final Map<String, String> properties;

        public CompiledCopyFromFileStatement(String databaseName, DataverseName dataverseName, String datasetName,
                Datatype itemType, String adapter, Map<String, String> properties) {
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.itemType = itemType;
            this.adapter = adapter;
            this.properties = properties;
        }

        @Override
        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public DataverseName getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public String getAdapter() {
            return adapter;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public Datatype getItemType() {
            return itemType;
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.COPY_FROM;
        }

        @Override
        public byte getCategory() {
            return Statement.Category.UPDATE;
        }
    }

    public static class CompiledInsertStatement extends AbstractCompiledStatement implements ICompiledDmlStatement {

        private final String databaseName;
        private final DataverseName dataverseName;
        private final String datasetName;
        private final Query query;
        private final int varCounter;
        private final VariableExpr var;
        private final Expression returnExpression;

        public CompiledInsertStatement(String databaseName, DataverseName dataverseName, String datasetName,
                Query query, int varCounter, VariableExpr var, Expression returnExpression) {
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
            this.var = var;
            this.returnExpression = returnExpression;
        }

        @Override
        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public DataverseName getDataverseName() {
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

        @Override
        public byte getCategory() {
            return Statement.Category.UPDATE;
        }
    }

    public static class CompiledUpsertStatement extends CompiledInsertStatement {

        public CompiledUpsertStatement(String databaseName, DataverseName dataverseName, String datasetName,
                Query query, int varCounter, VariableExpr var, Expression returnExpression) {
            super(databaseName, dataverseName, datasetName, query, varCounter, var, returnExpression);
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.UPSERT;
        }
    }

    public static class CompiledSubscribeFeedStatement extends AbstractCompiledStatement
            implements ICompiledDmlStatement {

        private final FeedConnectionRequest request;
        private final int varCounter;

        public CompiledSubscribeFeedStatement(FeedConnectionRequest request, int varCounter) {
            this.request = request;
            this.varCounter = varCounter;
        }

        @Override
        public String getDatabaseName() {
            return request.getReceivingFeedId().getDatabaseName();
        }

        @Override
        public DataverseName getDataverseName() {
            return request.getReceivingFeedId().getDataverseName();
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

        @Override
        public byte getCategory() {
            return Statement.Category.UPDATE;
        }
    }

    public static class CompiledDeleteStatement extends AbstractCompiledStatement implements ICompiledDmlStatement {

        private final String databaseName;
        private final DataverseName dataverseName;
        private final String datasetName;
        private final Expression condition;
        private final int varCounter;
        private final Query query;

        public CompiledDeleteStatement(VariableExpr var, String databaseName, DataverseName dataverseName,
                String datasetName, Expression condition, int varCounter, Query query) {
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.condition = condition;
            this.varCounter = varCounter;
            this.query = query;
        }

        @Override
        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public DataverseName getDataverseName() {
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

        @Override
        public byte getCategory() {
            return Statement.Category.UPDATE;
        }
    }

    public static class CompiledCompactStatement extends AbstractCompiledStatement {

        private final String databaseName;
        private final DataverseName dataverseName;
        private final String datasetName;

        public CompiledCompactStatement(String databaseName, DataverseName dataverseName, String datasetName) {
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public DataverseName getDataverseName() {
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
        private final Dataset dataset;
        private final Index index;

        public CompiledIndexCompactStatement(Dataset dataset, Index index) {
            super(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
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

    public static class CompiledCopyToStatement extends AbstractCompiledStatement {
        private final Query query;
        private final VariableExpr sourceVariable;
        private final String adapter;
        private final Map<String, String> properties;
        private final List<Expression> pathExpressions;
        private final List<Expression> partitionExpressions;
        private final Map<Integer, VariableExpr> partitionsVariables;
        private final List<Expression> orderbyList;
        private final List<OrderbyClause.OrderModifier> orderByModifiers;
        private final List<OrderbyClause.NullOrderModifier> orderByNullModifierList;

        public CompiledCopyToStatement(CopyToStatement copyToStatement) {
            this.query = copyToStatement.getQuery();
            this.sourceVariable = copyToStatement.getSourceVariable();
            ExternalDetailsDecl eddDecl = copyToStatement.getExternalDetailsDecl();
            this.adapter = eddDecl.getAdapter();
            this.properties = eddDecl.getProperties();
            this.pathExpressions = copyToStatement.getPathExpressions();
            this.partitionExpressions = copyToStatement.getPartitionExpressions();
            this.partitionsVariables = copyToStatement.getPartitionsVariables();
            this.orderbyList = copyToStatement.getOrderByList();
            this.orderByModifiers = copyToStatement.getOrderByModifiers();
            this.orderByNullModifierList = copyToStatement.getOrderByNullModifierList();
        }

        @Override
        public Statement.Kind getKind() {
            return Statement.Kind.COPY_TO;
        }

        public Query getQuery() {
            return query;
        }

        public VariableExpr getSourceVariable() {
            return sourceVariable;
        }

        public String getAdapter() {
            return adapter;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public List<Expression> getPathExpressions() {
            return pathExpressions;
        }

        public boolean isPartitioned() {
            return !partitionExpressions.isEmpty();
        }

        public boolean isOrdered() {
            return !orderbyList.isEmpty();
        }

        public List<Expression> getPartitionExpressions() {
            return partitionExpressions;
        }

        public VariableExpr getPartitionsVariables(int index) {
            return partitionsVariables.get(index);
        }

        public List<Expression> getOrderByExpressions() {
            return orderbyList;
        }

        public List<OrderbyClause.OrderModifier> getOrderByModifiers() {
            return orderByModifiers;
        }

        public List<OrderbyClause.NullOrderModifier> getOrderByNullModifiers() {
            return orderByNullModifierList;
        }
    }

}
