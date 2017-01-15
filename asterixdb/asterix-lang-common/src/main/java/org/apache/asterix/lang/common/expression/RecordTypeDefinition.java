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
package org.apache.asterix.lang.common.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.annotations.IRecordFieldDataGen;
import org.apache.asterix.common.annotations.UndeclaredFieldsDataGen;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.lang3.ObjectUtils;

public class RecordTypeDefinition implements TypeExpression {

    public enum RecordKind {
        OPEN,
        CLOSED
    }

    private final List<String> fieldNames = new ArrayList<>();
    private final List<TypeExpression> fieldTypes = new ArrayList<>();
    private final List<IRecordFieldDataGen> fieldDataGen = new ArrayList<>();
    private final List<Boolean> optionalFields = new ArrayList<>();
    private RecordKind recordKind;
    private UndeclaredFieldsDataGen undeclaredFieldsDataGen;

    public RecordTypeDefinition() {
        // Default constructor.
    }

    @Override
    public TypeExprKind getTypeKind() {
        return TypeExprKind.RECORD;
    }

    public void addField(String name, TypeExpression type, Boolean nullable, IRecordFieldDataGen fldDataGen) {
        fieldNames.add(name);
        fieldTypes.add(type);
        optionalFields.add(nullable);
        fieldDataGen.add(fldDataGen);
    }

    public void addField(String name, TypeExpression type, Boolean optional) {
        fieldNames.add(name);
        fieldTypes.add(type);
        optionalFields.add(optional);
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<TypeExpression> getFieldTypes() {
        return fieldTypes;
    }

    public List<Boolean> getOptionableFields() {
        return optionalFields;
    }

    public List<IRecordFieldDataGen> getFieldDataGen() {
        return fieldDataGen;
    }

    public RecordKind getRecordKind() {
        return recordKind;
    }

    public void setRecordKind(RecordKind recordKind) {
        this.recordKind = recordKind;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public void setUndeclaredFieldsDataGen(UndeclaredFieldsDataGen undeclaredFieldsDataGen) {
        this.undeclaredFieldsDataGen = undeclaredFieldsDataGen;
    }

    public UndeclaredFieldsDataGen getUndeclaredFieldsDataGen() {
        return undeclaredFieldsDataGen;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(fieldDataGen, fieldNames, fieldTypes, optionalFields, recordKind,
                undeclaredFieldsDataGen);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RecordTypeDefinition)) {
            return false;
        }
        RecordTypeDefinition target = (RecordTypeDefinition) object;
        boolean equals = fieldDataGen.equals(target.getFieldDataGen()) && fieldNames.equals(target.getFieldNames())
                && fieldTypes.equals(target.getFieldNames()) && optionalFields.equals(target.getOptionableFields());
        equals = equals && ObjectUtils.equals(recordKind, target.getRecordKind())
                && ObjectUtils.equals(undeclaredFieldsDataGen, target.getUndeclaredFieldsDataGen());
        return equals;
    }

}
