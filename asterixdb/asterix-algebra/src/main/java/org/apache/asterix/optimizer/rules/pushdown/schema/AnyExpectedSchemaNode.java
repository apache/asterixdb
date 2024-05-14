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

public class AnyExpectedSchemaNode extends AbstractExpectedSchemaNode {
    private boolean replaceable;

    public AnyExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            String functionName) {
        super(parent, sourceLocation, functionName);
        replaceable = true;
    }

    protected AnyExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            String functionName, boolean replaceable) {
        super(parent, sourceLocation, functionName);
        this.replaceable = replaceable;
    }

    @Override
    public boolean allowsReplacing() {
        return replaceable;
    }

    public void preventReplacing() {
        replaceable = false;
    }

    @Override
    public IExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType, SourceLocation sourceLocation,
            String functionName) {
        if (expectedNodeType == ExpectedSchemaNodeType.ANY) {
            return this;
        }
        /*
         * ANY node is typeless (i.e., we do not know what is the possible type of ANY node) when we created it.
         * However, now the query says it is (possibly) a nested value. We know that because there is a field
         * access expression or an array access expression on that node. So, we should replace the ANY node to
         * the given nested type.
         */
        AbstractComplexExpectedSchemaNode parent = getParent();
        AbstractComplexExpectedSchemaNode nestedNode = AbstractComplexExpectedSchemaNode
                .createNestedNode(expectedNodeType, parent, getSourceLocation(), functionName);
        return parent.replaceChild(this, nestedNode);
    }

    @Override
    public ExpectedSchemaNodeType getType() {
        return ExpectedSchemaNodeType.ANY;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }
}
