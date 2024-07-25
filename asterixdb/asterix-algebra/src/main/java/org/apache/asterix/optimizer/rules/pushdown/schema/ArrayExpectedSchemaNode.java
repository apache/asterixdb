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

import org.apache.hyracks.api.exceptions.SourceLocation;

public class ArrayExpectedSchemaNode extends AbstractComplexExpectedSchemaNode {
    private IExpectedSchemaNode child;

    public ArrayExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            String functionName) {
        super(parent, sourceLocation, functionName);
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.ARRAY;
    }

    public IExpectedSchemaNode getChild() {
        return child;
    }

    public void addChild(IExpectedSchemaNode child) {
        this.child = child;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public void replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode) {
        if (oldNode != child) {
            //this should not happen
            throw new IllegalStateException("Node " + oldNode.getType() + " is not a child");
        }
        child = newNode;
    }
}
