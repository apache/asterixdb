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
package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.annotations.TypeDataGen;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class TypeDecl extends AbstractStatement {

    private final Namespace namespace;
    private final Identifier ident;
    private final TypeExpression typeDef;
    private final TypeDataGen datagenAnnotation;
    private final boolean ifNotExists;

    public TypeDecl(Namespace namespace, Identifier ident, TypeExpression typeDef, TypeDataGen datagen,
            boolean ifNotExists) {
        this.namespace = namespace;
        this.ident = ident;
        this.typeDef = typeDef;
        this.datagenAnnotation = datagen;
        this.ifNotExists = ifNotExists;
    }

    public Identifier getIdent() {
        return ident;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public TypeExpression getTypeDef() {
        return typeDef;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.TYPE_DECL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public TypeDataGen getDatagenAnnotation() {
        return datagenAnnotation;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

}
