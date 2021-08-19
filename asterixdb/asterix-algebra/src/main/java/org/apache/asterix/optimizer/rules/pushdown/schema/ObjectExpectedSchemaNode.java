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
import java.util.Set;

import org.apache.hyracks.api.exceptions.SourceLocation;

public class ObjectExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private final Map<String, IExpectedSchemaNode> children;

    ObjectExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            String functionName) {
        super(parent, sourceLocation, functionName);
        children = new HashMap<>();
    }

    public Set<Map.Entry<String, IExpectedSchemaNode>> getChildren() {
        return children.entrySet();
    }

    public IExpectedSchemaNode addChild(String fieldName, IExpectedSchemaNode child) {
        children.put(fieldName, child);
        return child;
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
        String key = null;
        for (Map.Entry<String, IExpectedSchemaNode> child : children.entrySet()) {
            if (child.getValue() == oldNode) {
                key = child.getKey();
                break;
            }
        }

        if (key == null) {
            //this should not happen
            throw new IllegalStateException("Node " + oldNode.getType() + " is not a child");
        }
        children.replace(key, newNode);
    }
}
