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
package org.apache.asterix.optimizer.rules.pushdown.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ObjectExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private final Map<String, IExpectedSchemaNode> children;

    public ObjectExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            String functionName) {
        super(parent, sourceLocation, functionName);
        children = new HashMap<>();
    }

    public boolean isRoot() {
        return false;
    }

    public Map<String, IExpectedSchemaNode> getChildren() {
        return children;
    }

    public void addChild(String fieldName, IExpectedSchemaNode child) {
        children.put(fieldName, child);
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.OBJECT;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public void replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode) {
        String fieldName = getChildFieldName(oldNode);
        children.replace(fieldName, newNode);
    }

    public String getChildFieldName(IExpectedSchemaNode requestedChild) {
        String key = null;
        for (Map.Entry<String, IExpectedSchemaNode> child : children.entrySet()) {
            if (child.getValue() == requestedChild) {
                key = child.getKey();
                break;
            }
        }

        if (key == null) {
            //this should not happen
            throw new IllegalStateException("Node " + requestedChild.getType() + " is not a child");
        }
        return key;
    }

    protected IAType getType(IAType childType, IExpectedSchemaNode childNode, String typeName) {
        String key = getChildFieldName(childNode);
        IAType[] fieldTypes = { childType };
        String[] fieldNames = { key };

        return new ARecordType("typeName", fieldNames, fieldTypes, false);
    }
}
