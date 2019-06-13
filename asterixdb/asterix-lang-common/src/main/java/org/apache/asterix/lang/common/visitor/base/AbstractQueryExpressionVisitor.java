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
package org.apache.asterix.lang.common.visitor.base;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.clause.UpdateClause;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;

public abstract class AbstractQueryExpressionVisitor<R, T> implements ILangVisitor<R, T> {

    @Override
    public R visit(Query q, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FunctionDecl fd, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CreateIndexStatement cis, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DataverseDecl dv, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DeleteStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DropDatasetStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DatasetDecl dd, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(InsertStatement insert, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(LoadStatement stmtLoad, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(NodegroupDecl ngd, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(OrderedListTypeDefinition olte, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(RecordTypeDefinition tre, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(SetStatement ss, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(TypeDecl td, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(TypeReferenceExpression tre, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(UnorderedListTypeDefinition ulte, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(UpdateClause del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(UpdateStatement update, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(WriteStatement ws, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CreateDataverseStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(IndexDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(NodeGroupDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DataverseDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(TypeDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DisconnectFeedStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CreateFunctionStatement cfs, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FunctionDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CreateFeedStatement cfs, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(StartFeedStatement sfs, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(StopFeedStatement sfs, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(ConnectFeedStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FeedDropStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CompactStatement del, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CreateFeedPolicyStatement cfps, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FeedPolicyDropStatement dfs, T arg) throws CompilationException {
        return null;
    }
}
