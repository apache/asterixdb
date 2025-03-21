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
package org.apache.asterix.lang.sqlpp.rewrites;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppCopyToRewriteVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppDeleteRewriteVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSynonymRewriteVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;

class SqlppStatementRewriter implements IStatementRewriter {

    @Override
    public boolean isRewritable(Statement.Kind kind) {
        switch (kind) {
            case LOAD:
            case INSERT:
            case UPSERT:
            case DELETE:
            case COPY_TO:
            case TRUNCATE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public void rewrite(Statement stmt, MetadataProvider metadataProvider) throws CompilationException {
        if (stmt != null) {
            stmt.accept(SqlppSynonymRewriteVisitor.INSTANCE, metadataProvider);
            stmt.accept(SqlppDeleteRewriteVisitor.INSTANCE, metadataProvider);
            stmt.accept(SqlppCopyToRewriteVisitor.INSTANCE, metadataProvider);
        }
    }
}
