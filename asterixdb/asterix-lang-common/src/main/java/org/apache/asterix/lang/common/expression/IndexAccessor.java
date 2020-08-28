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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class IndexAccessor extends AbstractAccessor {

    public enum IndexKind {
        ELEMENT,
        ANY,
        STAR
    }

    private IndexKind indexKind;

    private Expression indexExpr;

    public IndexAccessor(Expression expr, IndexKind indexKind, Expression indexExpr) {
        super(expr);
        if (indexKind != IndexKind.ELEMENT && indexExpr != null) {
            throw new IllegalArgumentException(indexExpr.toString());
        }
        this.indexKind = Objects.requireNonNull(indexKind);
        this.indexExpr = indexExpr;
    }

    public IndexKind getIndexKind() {
        return indexKind;
    }

    public void setIndexKind(IndexKind indexKind) {
        this.indexKind = indexKind;
    }

    public Expression getIndexExpr() {
        return indexExpr;
    }

    public void setIndexExpr(Expression indexExpr) {
        this.indexExpr = indexExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.INDEX_ACCESSOR_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(indexKind, indexExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IndexAccessor)) {
            return false;
        }
        IndexAccessor target = (IndexAccessor) object;
        return super.equals(target) && indexKind == target.indexKind && Objects.equals(indexExpr, target.indexExpr);
    }

    @Override
    public String toString() {
        return expr + "[" + indexKind + '(' + indexExpr + ")]";
    }
}
