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

package org.apache.asterix.common.annotations;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Contains an ordered set of fields of a record
 */
public final class RecordFieldOrderAnnotation implements IRecordTypeAnnotation, Serializable {

    private static final long serialVersionUID = 1L;

    private final LinkedHashSet<String> fieldNames;

    public RecordFieldOrderAnnotation(LinkedHashSet<String> fieldNames) {
        this.fieldNames = Objects.requireNonNull(fieldNames);
    }

    public LinkedHashSet<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public Kind getKind() {
        return Kind.RECORD_FIELD_ORDER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RecordFieldOrderAnnotation that = (RecordFieldOrderAnnotation) o;
        return fieldNames.equals(that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames);
    }
}
