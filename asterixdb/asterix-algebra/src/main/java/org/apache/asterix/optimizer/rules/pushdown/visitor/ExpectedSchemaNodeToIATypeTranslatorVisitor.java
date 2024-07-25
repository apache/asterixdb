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

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.pushdown.schema.AbstractComplexExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.AnyExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ArrayExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaNodeType;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.IExpectedSchemaNodeVisitor;
import org.apache.asterix.optimizer.rules.pushdown.schema.ObjectExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.optimizer.rules.pushdown.schema.UnionExpectedSchemaNode;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.asterix.runtime.projection.ProjectionFiltrationWarningFactoryProvider;

/**
 * This visitor translates the {@link IExpectedSchemaNode} to {@link IAType} record.
 * The {@link IAType#getTypeName()} is used to map each {@link IAType} to its {@link FunctionCallInformation}
 */
public class ExpectedSchemaNodeToIATypeTranslatorVisitor implements IExpectedSchemaNodeVisitor<IAType, String> {
    //Map typeName to source information
    private final Map<String, FunctionCallInformation> sourceInformationMap;
    //To give a unique name for each type
    private int counter;

    public ExpectedSchemaNodeToIATypeTranslatorVisitor(Map<String, FunctionCallInformation> sourceInformationMap) {
        this.sourceInformationMap = sourceInformationMap;
    }

    @Override
    public IAType visit(RootExpectedSchemaNode node, String arg) {
        if (node.isAllFields()) {
            return ALL_FIELDS_TYPE;
        } else if (node.isEmpty()) {
            return EMPTY_TYPE;
        }
        return createRecordType(node, String.valueOf(counter++));
    }

    @Override
    public IAType visit(ObjectExpectedSchemaNode node, String arg) {
        IAType recordType = createRecordType(node, arg);
        sourceInformationMap.put(arg, createFunctionCallInformation(node));
        return recordType;
    }

    @Override
    public IAType visit(ArrayExpectedSchemaNode node, String arg) {
        IAType itemType = node.getChild().accept(this, String.valueOf(counter++));
        IAType listType = new AOrderedListType(itemType, arg);
        sourceInformationMap.put(arg, createFunctionCallInformation(node));
        return listType;
    }

    @Override
    public IAType visit(UnionExpectedSchemaNode node, String arg) {
        List<IAType> unionTypes = new ArrayList<>();
        for (Map.Entry<ExpectedSchemaNodeType, AbstractComplexExpectedSchemaNode> child : node.getChildren()) {
            unionTypes.add(child.getValue().accept(this, String.valueOf(counter++)));
        }
        IAType unionType = new AUnionType(unionTypes, arg);
        sourceInformationMap.put(arg, createFunctionCallInformation(node));
        return unionType;
    }

    @Override
    public IAType visit(AnyExpectedSchemaNode node, String arg) {
        return BuiltinType.ANY;
    }

    private ARecordType createRecordType(ObjectExpectedSchemaNode node, String arg) {
        Map<String, IExpectedSchemaNode> children = node.getChildren();
        String[] childrenFieldNames = new String[children.size()];
        IAType[] childrenTypes = new IAType[children.size()];
        int i = 0;
        for (Map.Entry<String, IExpectedSchemaNode> child : children.entrySet()) {
            childrenFieldNames[i] = child.getKey();
            childrenTypes[i++] = child.getValue().accept(this, String.valueOf(counter++));
        }

        return new ARecordType(arg, childrenFieldNames, childrenTypes, true);
    }

    private FunctionCallInformation createFunctionCallInformation(IExpectedSchemaNode node) {
        return new FunctionCallInformation(node.getFunctionName(), node.getSourceLocation(),
                ProjectionFiltrationWarningFactoryProvider.TYPE_MISMATCH_FACTORY);
    }
}
