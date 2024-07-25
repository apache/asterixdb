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
package org.apache.asterix.optimizer.rules.pushdown.descriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.visitor.SimpleStringBuilderForIATypeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.visitor.ExpectedSchemaNodeToIATypeTranslatorVisitor;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;

public class ScanDefineDescriptor extends DefineDescriptor {
    private final Dataset dataset;
    private final List<LogicalVariable> primaryKeyVariables;
    private final LogicalVariable metaRecordVariable;
    private final Map<ILogicalExpression, ARecordType> paths;
    private final Map<String, FunctionCallInformation> pathLocations;
    private RootExpectedSchemaNode recordNode;
    private RootExpectedSchemaNode metaNode;
    private ILogicalExpression filterExpression;
    private ILogicalExpression rangeFilterExpression;

    public ScanDefineDescriptor(int scope, Dataset dataset, List<LogicalVariable> primaryKeyVariables,
            LogicalVariable recordVariable, LogicalVariable metaRecordVariable, ILogicalOperator operator) {
        super(scope, null, recordVariable, operator, null, -1);
        this.primaryKeyVariables = primaryKeyVariables;
        this.metaRecordVariable = metaRecordVariable;
        this.dataset = dataset;
        paths = new HashMap<>();
        pathLocations = new HashMap<>();

        recordNode = RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE;
        if (hasMeta()) {
            metaNode = RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE;
        }
    }

    @Override
    public boolean isScanDefinition() {
        return true;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public List<LogicalVariable> getPrimaryKeyVariables() {
        return primaryKeyVariables;
    }

    public boolean hasMeta() {
        return metaRecordVariable != null;
    }

    public LogicalVariable getMetaRecordVariable() {
        return metaRecordVariable;
    }

    public void setRecordNode(RootExpectedSchemaNode recordNode) {
        this.recordNode = recordNode;
    }

    public RootExpectedSchemaNode getRecordNode() {
        return recordNode;
    }

    public void setMetaNode(RootExpectedSchemaNode metaNode) {
        this.metaNode = metaNode;
    }

    public RootExpectedSchemaNode getMetaNode() {
        return metaNode;
    }

    public Map<ILogicalExpression, ARecordType> getFilterPaths() {
        return paths;
    }

    public Map<String, FunctionCallInformation> getPathLocations() {
        return pathLocations;
    }

    public void setFilterExpression(ILogicalExpression expression) {
        this.filterExpression = expression;
    }

    public ILogicalExpression getFilterExpression() {
        return filterExpression;
    }

    public void setRangeFilterExpression(ILogicalExpression rangeFilterExpression) {
        this.rangeFilterExpression = rangeFilterExpression;
    }

    public ILogicalExpression getRangeFilterExpression() {
        return rangeFilterExpression;
    }

    @Override
    public String toString() {
        ExpectedSchemaNodeToIATypeTranslatorVisitor converter =
                new ExpectedSchemaNodeToIATypeTranslatorVisitor(new HashMap<>());
        SimpleStringBuilderForIATypeVisitor typeStringVisitor = new SimpleStringBuilderForIATypeVisitor();
        StringBuilder builder = new StringBuilder();

        AbstractScanOperator scanOp = (AbstractScanOperator) operator;
        builder.append("[record: ");
        builder.append(getVariable());
        if (hasMeta()) {
            builder.append(", meta: ");
            builder.append(metaRecordVariable);
        }
        builder.append("] <- ");
        builder.append(scanOp.getOperatorTag());

        builder.append('\n');
        boolean fieldAccessPushdown = DatasetUtil.isFieldAccessPushdownSupported(dataset);
        if (fieldAccessPushdown && !recordNode.isAllFields()) {
            builder.append("project: ");
            ARecordType recordType = (ARecordType) recordNode.accept(converter, "root");
            recordType.accept(typeStringVisitor, builder);

            if (hasMeta()) {
                builder.append(" project-meta: ");
                ARecordType metaType = (ARecordType) metaNode.accept(converter, "meta");
                metaType.accept(typeStringVisitor, builder);
            }
        }

        builder.append('\n');
        if (filterExpression != null) {
            builder.append("filter: ");
            builder.append(filterExpression);
        }

        builder.append('\n');
        if (rangeFilterExpression != null) {
            builder.append("range-filter: ");
            builder.append(rangeFilterExpression);
        }

        return builder.toString();
    }
}
