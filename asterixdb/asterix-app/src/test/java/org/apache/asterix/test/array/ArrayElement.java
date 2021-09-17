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
package org.apache.asterix.test.array;

import java.util.ArrayList;
import java.util.List;

public class ArrayElement {
    final List<TableField> unnestList = new ArrayList<>();
    final List<TableField> projectList = new ArrayList<>();
    final int elementPosition;
    final Kind kind;

    public ArrayElement(Kind kind, int elementPosition) {
        this.elementPosition = elementPosition;
        this.kind = kind;
    }

    public

    static class TableField {
        private final List<String> fieldName;
        private final BaseWisconsinTable.Field sourceField;

        TableField(List<String> name, BaseWisconsinTable.Field field) {
            fieldName = name;
            sourceField = field;
        }

        List<String> getFieldName() {
            return fieldName;
        }

        String getFullFieldName() {
            return String.join(".", fieldName);
        }

        String getLastFieldName() {
            return fieldName.get(fieldName.size() - 1);
        }

        BaseWisconsinTable.Field getSourceField() {
            return sourceField;
        }

        public BaseWisconsinTable.Field.Type getFieldType() {
            return (sourceField == null) ? null : sourceField.fieldType;
        }

        @Override
        public String toString() {
            return String.format("%s [%s]", getFullFieldName(), (sourceField == null) ? "NONE" : sourceField.fieldName);
        }
    }

    enum Kind {
        ATOMIC,
        UNNEST_VALUE,
        UNNEST_OBJECT
    }
}
