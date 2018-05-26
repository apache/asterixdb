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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class UnorderedListTypeDefinition extends AbstractTypeExpression {

    private TypeExpression itemTypeExpression;

    public UnorderedListTypeDefinition(TypeExpression itemTypeExpression) {
        this.itemTypeExpression = itemTypeExpression;
    }

    @Override
    public TypeExprKind getTypeKind() {
        return TypeExprKind.UNORDEREDLIST;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public TypeExpression getItemTypeExpression() {
        return itemTypeExpression;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(itemTypeExpression);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UnorderedListTypeDefinition)) {
            return false;
        }
        UnorderedListTypeDefinition target = (UnorderedListTypeDefinition) object;
        return Objects.equals(itemTypeExpression, target.itemTypeExpression);
    }
}
