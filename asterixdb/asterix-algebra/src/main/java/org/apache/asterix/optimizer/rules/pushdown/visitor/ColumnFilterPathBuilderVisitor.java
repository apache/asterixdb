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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.renameType;

import java.util.Map;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNodeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.asterix.runtime.projection.ProjectionFiltrationWarningFactoryProvider;

public class ColumnFilterPathBuilderVisitor implements IExpectedSchemaNodeVisitor<IAType, IExpectedSchemaNode> {

    private IAType type;
    private Map<String, FunctionCallInformation> sourceInformationMap;
    private int counter = 0;

    public ARecordType buildPath(AnyExpectedSchemaNode anyNode) {
        return buildPath(anyNode, BuiltinType.ANY, null, null);
    }

    public ARecordType buildPath(AnyExpectedSchemaNode anyNode, IAType constType,
            Map<String, FunctionCallInformation> sourceInformationMap, FunctionCallInformation compareFunctionInfo) {

        this.sourceInformationMap = sourceInformationMap;
        this.type = constType;
        if (sourceInformationMap != null) {
            this.type = renameType(constType, getTypeName());
            sourceInformationMap.put(type.getTypeName(), compareFunctionInfo);
        }
        return (ARecordType) anyNode.accept(this, anyNode);
    }

    @Override
    public IAType visit(RootExpectedSchemaNode node, IExpectedSchemaNode arg) {
        type = getRecordType(node, type, arg, getTypeName());
        return type;
    }

    @Override
    public IAType visit(ObjectExpectedSchemaNode node, IExpectedSchemaNode arg) {
        type = getRecordType(node, type, arg, getTypeName());
        putCallInfo(type, arg);
        return node.getParent().accept(this, node);
    }

    @Override
    public IAType visit(ArrayExpectedSchemaNode node, IExpectedSchemaNode arg) {
        type = new AOrderedListType(type, getTypeName());
        putCallInfo(type, arg);
        return node.getParent().accept(this, node);
    }

    @Override
    public IAType visit(UnionExpectedSchemaNode node, IExpectedSchemaNode arg) {
        putCallInfo(type, arg);
        return node.getParent().accept(this, arg);
    }

    @Override
    public IAType visit(AnyExpectedSchemaNode node, IExpectedSchemaNode arg) {
        return node.getParent().accept(this, node);
    }

    private void putCallInfo(IAType type, IExpectedSchemaNode node) {
        if (sourceInformationMap != null) {
            sourceInformationMap.put(type.getTypeName(), createFunctionCallInformation(node));
        }
    }

    private static ARecordType getRecordType(ObjectExpectedSchemaNode objectNode, IAType childType,
            IExpectedSchemaNode childNode, String typeName) {
        String key = objectNode.getChildFieldName(childNode);
        IAType[] fieldTypes = { childType };
        String[] fieldNames = { key };

        return new ARecordType(typeName, fieldNames, fieldTypes, true);
    }

    private String getTypeName() {
        return "FilterPath" + counter++;
    }

    private FunctionCallInformation createFunctionCallInformation(IExpectedSchemaNode node) {
        return new FunctionCallInformation(node.getFunctionName(), node.getSourceLocation(),
                ProjectionFiltrationWarningFactoryProvider.TYPE_MISMATCH_FACTORY);
    }
}
