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

import java.net.URI;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public final class CreateLibraryStatement extends AbstractStatement {

    private final DataverseName dataverseName;
    private final String libraryName;
    private final ExternalFunctionLanguage lang;
    private final URI location;
    private final boolean replaceIfExists;
    private final String authToken;

    public CreateLibraryStatement(DataverseName dataverseName, String libraryName, ExternalFunctionLanguage lang,
            URI location, boolean replaceIfExists, String authToken) {
        this.dataverseName = dataverseName;
        this.libraryName = libraryName;
        this.lang = lang;
        this.location = location;
        this.replaceIfExists = replaceIfExists;
        this.authToken = authToken;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public ExternalFunctionLanguage getLang() {
        return lang;
    }

    public URI getLocation() {
        return location;
    }

    public boolean getReplaceIfExists() {
        return replaceIfExists;
    }

    public String getAuthToken() {
        return authToken;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_LIBRARY;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }
}