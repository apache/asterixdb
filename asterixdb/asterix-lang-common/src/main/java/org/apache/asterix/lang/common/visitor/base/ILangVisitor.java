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
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.UpdateClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.VariableExpr;
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

public interface ILangVisitor<R, T> {

    R visit(Query q, T arg) throws CompilationException;

    R visit(FunctionDecl fd, T arg) throws CompilationException;

    R visit(TypeDecl td, T arg) throws CompilationException;

    R visit(NodegroupDecl ngd, T arg) throws CompilationException;

    R visit(DatasetDecl dd, T arg) throws CompilationException;

    R visit(LoadStatement stmtLoad, T arg) throws CompilationException;

    R visit(DropDatasetStatement del, T arg) throws CompilationException;

    R visit(InsertStatement insert, T arg) throws CompilationException;

    R visit(DeleteStatement del, T arg) throws CompilationException;

    R visit(UpdateStatement update, T arg) throws CompilationException;

    R visit(UpdateClause del, T arg) throws CompilationException;

    R visit(TypeReferenceExpression tre, T arg) throws CompilationException;

    R visit(RecordTypeDefinition tre, T arg) throws CompilationException;

    R visit(OrderedListTypeDefinition olte, T arg) throws CompilationException;

    R visit(UnorderedListTypeDefinition ulte, T arg) throws CompilationException;

    R visit(LiteralExpr l, T arg) throws CompilationException;

    R visit(VariableExpr v, T arg) throws CompilationException;

    R visit(ListConstructor lc, T arg) throws CompilationException;

    R visit(RecordConstructor rc, T arg) throws CompilationException;

    R visit(OperatorExpr ifbo, T arg) throws CompilationException;

    R visit(FieldAccessor fa, T arg) throws CompilationException;

    R visit(IndexAccessor ia, T arg) throws CompilationException;

    R visit(IfExpr ifexpr, T arg) throws CompilationException;

    R visit(QuantifiedExpression qe, T arg) throws CompilationException;

    R visit(LetClause lc, T arg) throws CompilationException;

    R visit(WhereClause wc, T arg) throws CompilationException;

    R visit(OrderbyClause oc, T arg) throws CompilationException;

    R visit(GroupbyClause gc, T arg) throws CompilationException;

    R visit(LimitClause lc, T arg) throws CompilationException;

    R visit(UnaryExpr u, T arg) throws CompilationException;

    R visit(CreateIndexStatement cis, T arg) throws CompilationException;

    R visit(CreateDataverseStatement del, T arg) throws CompilationException;

    R visit(IndexDropStatement del, T arg) throws CompilationException;

    R visit(NodeGroupDropStatement del, T arg) throws CompilationException;

    R visit(DataverseDropStatement del, T arg) throws CompilationException;

    R visit(TypeDropStatement del, T arg) throws CompilationException;

    R visit(WriteStatement ws, T arg) throws CompilationException;

    R visit(SetStatement ss, T arg) throws CompilationException;

    R visit(DisconnectFeedStatement del, T arg) throws CompilationException;

    R visit(ConnectFeedStatement del, T arg) throws CompilationException;

    R visit(StartFeedStatement sfs, T arg) throws CompilationException;

    R visit(StopFeedStatement sfs, T arg) throws CompilationException;

    R visit(CreateFeedStatement cfs, T arg) throws CompilationException;

    R visit(FeedDropStatement del, T arg) throws CompilationException;

    R visit(FeedPolicyDropStatement dfs, T arg) throws CompilationException;

    R visit(CreateFeedPolicyStatement cfps, T arg) throws CompilationException;

    R visit(CallExpr pf, T arg) throws CompilationException;

    R visit(DataverseDecl dv, T arg) throws CompilationException;

    R visit(CreateFunctionStatement cfs, T arg) throws CompilationException;

    R visit(FunctionDropStatement del, T arg) throws CompilationException;

    R visit(CompactStatement del, T arg) throws CompilationException;

    R visit(ListSliceExpression expression, T arg) throws CompilationException;
}
