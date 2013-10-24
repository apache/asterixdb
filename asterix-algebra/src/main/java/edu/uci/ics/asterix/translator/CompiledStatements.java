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
package edu.uci.ics.asterix.translator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.expression.CallExpr;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement.OperationType;
import edu.uci.ics.asterix.aql.expression.FLWOGRExpression;
import edu.uci.ics.asterix.aql.expression.FieldAccessor;
import edu.uci.ics.asterix.aql.expression.FieldBinding;
import edu.uci.ics.asterix.aql.expression.ForClause;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.literal.StringLiteral;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

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
        private final List<String> keyFields;
        private final IndexType indexType;

        // Specific to NGram index.
        private final int gramLength;

        public CompiledCreateIndexStatement(String indexName, String dataverseName, String datasetName,
                List<String> keyFields, int gramLength, IndexType indexType) {
            this.indexName = indexName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.keyFields = keyFields;
            this.gramLength = gramLength;
            this.indexType = indexType;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public int getGramLength() {
            return gramLength;
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

        public String getDataverseName() {
            return dataverseName;
        }

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
            return Kind.LOAD_FROM_FILE;
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

        public String getDataverseName() {
            return dataverseName;
        }

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

    public static class CompiledBeginFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private Query query;
        private int varCounter;

        public CompiledBeginFeedStatement(String dataverseName, String datasetName, Query query, int varCounter) {
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

        public void setQuery(Query query) {
            this.query = query;
        }

        @Override
        public Kind getKind() {
            return Kind.BEGIN_FEED;
        }
    }

    public static class CompiledControlFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private OperationType operationType;
        private Query query;
        private int varCounter;
        private Map<String, String> alteredParams;

        public CompiledControlFeedStatement(OperationType operationType, String dataverseName, String datasetName,
                Map<String, String> alteredParams) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.operationType = operationType;
            this.alteredParams = alteredParams;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.CONTROL_FEED;
        }

        public Map<String, String> getProperties() {
            return alteredParams;
        }

        public void setProperties(Map<String, String> properties) {
            this.alteredParams = properties;
        }
    }

    public static class CompiledDeleteStatement implements ICompiledDmlStatement {
        private VariableExpr var;
        private String dataverseName;
        private String datasetName;
        private Expression condition;
        private int varCounter;
        private AqlMetadataProvider metadataProvider;

        public CompiledDeleteStatement(VariableExpr var, String dataverseName, String datasetName,
                Expression condition, int varCounter, AqlMetadataProvider metadataProvider) {
            this.var = var;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.condition = condition;
            this.varCounter = varCounter;
            this.metadataProvider = metadataProvider;
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

            List<Expression> arguments = new ArrayList<Expression>();
            String arg = dataverseName == null ? datasetName : dataverseName + "." + datasetName;
            LiteralExpr argumentLiteral = new LiteralExpr(new StringLiteral(arg));
            arguments.add(argumentLiteral);

            CallExpr callExpression = new CallExpr(new FunctionSignature(FunctionConstants.ASTERIX_NS, "dataset", 1),
                    arguments);
            List<Clause> clauseList = new ArrayList<Clause>();
            Clause forClause = new ForClause(var, callExpression);
            clauseList.add(forClause);
            Clause whereClause = null;
            if (condition != null) {
                whereClause = new WhereClause(condition);
                clauseList.add(whereClause);
            }

            Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
            if (dataset == null) {
                throw new AlgebricksException("Unknown dataset " + datasetName);
            }
            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = metadataProvider.findType(dataset.getDataverseName(), itemTypeName);
            ARecordType recType = (ARecordType) itemType;
            String[] fieldNames = recType.getFieldNames();
            List<FieldBinding> fieldBindings = new ArrayList<FieldBinding>();
            for (int i = 0; i < fieldNames.length; i++) {
                FieldAccessor fa = new FieldAccessor(var, new Identifier(fieldNames[i]));
                FieldBinding fb = new FieldBinding(new LiteralExpr(new StringLiteral(fieldNames[i])), fa);
                fieldBindings.add(fb);
            }
            RecordConstructor rc = new RecordConstructor(fieldBindings);

            FLWOGRExpression flowgr = new FLWOGRExpression(clauseList, rc);
            Query query = new Query();
            query.setBody(flowgr);
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
        private final List<String> keyFields;
        private final IndexType indexType;

        // Specific to NGram index.
        private final int gramLength;

        public CompiledIndexCompactStatement(String dataverseName, String datasetName, String indexName,
                List<String> keyFields, int gramLength, IndexType indexType) {
            super(dataverseName, datasetName);
            this.indexName = indexName;
            this.keyFields = keyFields;
            this.gramLength = gramLength;
            this.indexType = indexType;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public int getGramLength() {
            return gramLength;
        }
    }

}
