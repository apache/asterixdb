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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class DataverseDecl extends AbstractStatement {

    private final Namespace namespace;

    private final boolean ifExists;

    public DataverseDecl(Namespace namespace) {
        this(namespace, false);
    }

    public DataverseDecl(Namespace namespace, boolean ifExists) {
        this.namespace = Objects.requireNonNull(namespace);
        this.ifExists = ifExists;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace.getDataverseName();
    }

    public String getDatabaseName() {
        return namespace.getDatabaseName();
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.DATAVERSE_DECL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }
}
