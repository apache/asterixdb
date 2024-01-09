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

public class RootExpectedSchemaNode extends ObjectExpectedSchemaNode {
    //Root with zero fields
    private static final int EMPTY_ROOT = 0;
    //Root with the entire fields
    private static final int ALL_FIELDS_ROOT = 1;
    //Root with custom fields
    private static final int CLIPPED_ROOT = 2;
    private static final int ALL_FIELDS_ROOT_IRREPLACEABLE = 3;
    public static final RootExpectedSchemaNode ALL_FIELDS_ROOT_NODE = new RootExpectedSchemaNode(ALL_FIELDS_ROOT);
    public static final RootExpectedSchemaNode ALL_FIELDS_ROOT_IRREPLACEABLE_NODE =
            new RootExpectedSchemaNode(ALL_FIELDS_ROOT_IRREPLACEABLE);
    public static final RootExpectedSchemaNode EMPTY_ROOT_NODE = new RootExpectedSchemaNode(EMPTY_ROOT);

    private final int rootType;

    RootExpectedSchemaNode() {
        this(CLIPPED_ROOT);
    }

    private RootExpectedSchemaNode(int rootType) {
        super(null, null, null);
        this.rootType = rootType;
    }

    @Override
    public boolean isRoot() {
        return true;
    }

    @Override
    public AbstractComplexExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType,
            SourceLocation sourceLocation, String functionName) {
        if (rootType == ALL_FIELDS_ROOT) {
            //ALL_FIELDS_ROOT. Return a new CLIPPED_ROOT root
            return new RootExpectedSchemaNode();
        }
        return this;
    }

    @Override
    public <R, T> R accept(IExpectedSchemaNodeVisitor<R, T> visitor, T arg) {
        return visitor.visit(this, arg);
    }

    public boolean isEmpty() {
        return rootType == EMPTY_ROOT;
    }

    public boolean isAllFields() {
        return rootType == ALL_FIELDS_ROOT || rootType == ALL_FIELDS_ROOT_IRREPLACEABLE;
    }
}
