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
package org.apache.asterix.lang.sqlpp.parser;

import java.io.Reader;

import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;

public class SqlppParserFactory implements IParserFactory {

    // WARNING: This value is stored in function metadata. Do not modify.
    public static final String SQLPP = "SQLPP";

    protected final INamespaceResolver namespaceResolver;

    public SqlppParserFactory(INamespaceResolver namespaceResolver) {
        this.namespaceResolver = namespaceResolver;
    }

    @Override
    public IParser createParser(String query) {
        return new SQLPPParser(query, namespaceResolver);
    }

    @Override
    public IParser createParser(Reader reader) {
        return new SQLPPParser(reader, namespaceResolver);
    }

    @Override
    public String getLanguage() {
        return SQLPP;
    }
}
