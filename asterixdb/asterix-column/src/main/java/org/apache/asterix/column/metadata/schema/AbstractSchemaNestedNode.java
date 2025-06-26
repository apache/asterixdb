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
package org.apache.asterix.column.metadata.schema;

public abstract class AbstractSchemaNestedNode extends AbstractSchemaNode {
    //A nestedNode can initially be empty, which contributes
    //to a missing column.
    protected boolean missingInitiallyInBatch;

    public void setMissingInitiallyInBatch(boolean missingInitiallyInBatch) {
        this.missingInitiallyInBatch = missingInitiallyInBatch;
    }

    public boolean isMissingInitiallyInBatch() {
        return missingInitiallyInBatch;
    }

    @Override
    public final boolean isNested() {
        return true;
    }
}
