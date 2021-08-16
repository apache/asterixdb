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

import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;

/**
 * This visitor produces a oneliner JSON-like representation of {@link IAType} to be interpreted by the user.
 */
public class SimpleStringBuilderForIATypeVisitor implements IATypeVisitor<Void, StringBuilder> {
    /**
     * Example: {"field1":string,"field2":[bigint]}
     */
    @Override
    public Void visit(ARecordType recordType, StringBuilder arg) {
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();

        arg.append("{");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                arg.append(',');
            }
            arg.append(fieldNames[i]);
            arg.append(':');
            fieldTypes[i].accept(this, arg);
        }
        arg.append("}");
        return null;
    }

    /**
     * Example:
     * - Array: [{"field1":bigint}]
     * - Multiset: {{bigint}}
     */
    @Override
    public Void visit(AbstractCollectionType collectionType, StringBuilder arg) {
        IAType itemType = collectionType.getItemType();

        arg.append(collectionType.getTypeTag() == ATypeTag.ARRAY ? "[" : "{{");
        itemType.accept(this, arg);
        arg.append(collectionType.getTypeTag() == ATypeTag.ARRAY ? "]" : "}}");
        return null;
    }

    /**
     * Example: A union type of array, object, and bigint
     * - <[{"field1":...}],{"field1:...},bigint>
     */
    @Override
    public Void visit(AUnionType unionType, StringBuilder arg) {
        List<IAType> unionList = unionType.getUnionList();

        arg.append("<");
        for (int i = 0; i < unionList.size(); i++) {
            if (i > 0) {
                arg.append(',');
            }
            unionList.get(i).accept(this, arg);
        }
        arg.append(">");
        return null;
    }

    /**
     * Example:
     * - bigint
     * - string
     */
    @Override
    public Void visitFlat(IAType flatType, StringBuilder arg) {
        arg.append(flatType.getTypeTag());
        return null;
    }
}
