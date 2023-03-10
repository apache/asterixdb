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
package org.apache.asterix.om.types.visitor;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

/**
 * Produces a path from a type
 */
public class PathStringBuilderForIATypeVisitor implements IATypeVisitor<Void, StringBuilder> {
    private boolean root = true;

    @Override
    public Void visit(ARecordType recordType, StringBuilder arg) {
        // here we assume the record type has only one child
        if (root) {
            arg.append("$$root");
            root = false;
        }
        arg.append('.');
        arg.append(recordType.getFieldNames()[0]);
        recordType.getFieldTypes()[0].accept(this, arg);
        return null;
    }

    @Override
    public Void visit(AbstractCollectionType collectionType, StringBuilder arg) {
        arg.append("[*]");
        collectionType.getItemType().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(AUnionType unionType, StringBuilder arg) {
        throw new NotImplementedException("Check how to represent this");
    }

    @Override
    public Void visitFlat(IAType flatType, StringBuilder arg) {
        return null;
    }
}
