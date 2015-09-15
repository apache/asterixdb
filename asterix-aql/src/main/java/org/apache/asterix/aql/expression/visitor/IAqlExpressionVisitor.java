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
package org.apache.asterix.aql.expression.visitor;

import org.apache.asterix.aql.expression.CallExpr;
import org.apache.asterix.aql.expression.CompactStatement;
import org.apache.asterix.aql.expression.ConnectFeedStatement;
import org.apache.asterix.aql.expression.CreateDataverseStatement;
import org.apache.asterix.aql.expression.CreateFeedPolicyStatement;
import org.apache.asterix.aql.expression.CreateFunctionStatement;
import org.apache.asterix.aql.expression.CreateIndexStatement;
import org.apache.asterix.aql.expression.CreatePrimaryFeedStatement;
import org.apache.asterix.aql.expression.CreateSecondaryFeedStatement;
import org.apache.asterix.aql.expression.DatasetDecl;
import org.apache.asterix.aql.expression.DataverseDecl;
import org.apache.asterix.aql.expression.DataverseDropStatement;
import org.apache.asterix.aql.expression.DeleteStatement;
import org.apache.asterix.aql.expression.DisconnectFeedStatement;
import org.apache.asterix.aql.expression.DistinctClause;
import org.apache.asterix.aql.expression.DropStatement;
import org.apache.asterix.aql.expression.FLWOGRExpression;
import org.apache.asterix.aql.expression.FeedDropStatement;
import org.apache.asterix.aql.expression.FeedPolicyDropStatement;
import org.apache.asterix.aql.expression.FieldAccessor;
import org.apache.asterix.aql.expression.ForClause;
import org.apache.asterix.aql.expression.FunctionDecl;
import org.apache.asterix.aql.expression.FunctionDropStatement;
import org.apache.asterix.aql.expression.GroupbyClause;
import org.apache.asterix.aql.expression.IfExpr;
import org.apache.asterix.aql.expression.IndexAccessor;
import org.apache.asterix.aql.expression.IndexDropStatement;
import org.apache.asterix.aql.expression.InsertStatement;
import org.apache.asterix.aql.expression.LetClause;
import org.apache.asterix.aql.expression.LimitClause;
import org.apache.asterix.aql.expression.ListConstructor;
import org.apache.asterix.aql.expression.LiteralExpr;
import org.apache.asterix.aql.expression.LoadStatement;
import org.apache.asterix.aql.expression.NodeGroupDropStatement;
import org.apache.asterix.aql.expression.NodegroupDecl;
import org.apache.asterix.aql.expression.OperatorExpr;
import org.apache.asterix.aql.expression.OrderbyClause;
import org.apache.asterix.aql.expression.OrderedListTypeDefinition;
import org.apache.asterix.aql.expression.QuantifiedExpression;
import org.apache.asterix.aql.expression.Query;
import org.apache.asterix.aql.expression.RecordConstructor;
import org.apache.asterix.aql.expression.RecordTypeDefinition;
import org.apache.asterix.aql.expression.SetStatement;
import org.apache.asterix.aql.expression.TypeDecl;
import org.apache.asterix.aql.expression.TypeDropStatement;
import org.apache.asterix.aql.expression.TypeReferenceExpression;
import org.apache.asterix.aql.expression.UnaryExpr;
import org.apache.asterix.aql.expression.UnionExpr;
import org.apache.asterix.aql.expression.UnorderedListTypeDefinition;
import org.apache.asterix.aql.expression.UpdateClause;
import org.apache.asterix.aql.expression.UpdateStatement;
import org.apache.asterix.aql.expression.VariableExpr;
import org.apache.asterix.aql.expression.WhereClause;
import org.apache.asterix.aql.expression.WriteStatement;
import org.apache.asterix.common.exceptions.AsterixException;

public interface IAqlExpressionVisitor<R, T> {

    R visitQuery(Query q, T arg) throws AsterixException;

    R visitFunctionDecl(FunctionDecl fd, T arg) throws AsterixException;

    R visitTypeDecl(TypeDecl td, T arg) throws AsterixException;

    R visitNodegroupDecl(NodegroupDecl ngd, T arg) throws AsterixException;

    R visitDatasetDecl(DatasetDecl dd, T arg) throws AsterixException;

    R visitLoadStatement(LoadStatement stmtLoad, T arg) throws AsterixException;

    R visitDropStatement(DropStatement del, T arg) throws AsterixException;

    R visitInsertStatement(InsertStatement insert, T arg) throws AsterixException;

    R visitDeleteStatement(DeleteStatement del, T arg) throws AsterixException;

    R visitUpdateStatement(UpdateStatement update, T arg) throws AsterixException;

    R visitUpdateClause(UpdateClause del, T arg) throws AsterixException;

    R visitTypeReferenceExpression(TypeReferenceExpression tre, T arg) throws AsterixException;

    R visitRecordTypeDefiniton(RecordTypeDefinition tre, T arg) throws AsterixException;

    R visitOrderedListTypeDefiniton(OrderedListTypeDefinition olte, T arg) throws AsterixException;

    R visitUnorderedListTypeDefiniton(UnorderedListTypeDefinition ulte, T arg) throws AsterixException;

    R visitLiteralExpr(LiteralExpr l, T arg) throws AsterixException;

    R visitVariableExpr(VariableExpr v, T arg) throws AsterixException;

    R visitListConstructor(ListConstructor lc, T arg) throws AsterixException;

    R visitRecordConstructor(RecordConstructor rc, T arg) throws AsterixException;

    R visitOperatorExpr(OperatorExpr ifbo, T arg) throws AsterixException;

    R visitFieldAccessor(FieldAccessor fa, T arg) throws AsterixException;

    R visitIndexAccessor(IndexAccessor ia, T arg) throws AsterixException;

    R visitIfExpr(IfExpr ifexpr, T arg) throws AsterixException;

    R visitFlworExpression(FLWOGRExpression flwor, T arg) throws AsterixException;

    R visitQuantifiedExpression(QuantifiedExpression qe, T arg) throws AsterixException;

    R visitForClause(ForClause fc, T arg) throws AsterixException;

    R visitLetClause(LetClause lc, T arg) throws AsterixException;

    R visitWhereClause(WhereClause wc, T arg) throws AsterixException;

    R visitOrderbyClause(OrderbyClause oc, T arg) throws AsterixException;

    R visitGroupbyClause(GroupbyClause gc, T arg) throws AsterixException;

    R visitLimitClause(LimitClause lc, T arg) throws AsterixException;

    R visitDistinctClause(DistinctClause dc, T arg) throws AsterixException;

    R visitUnaryExpr(UnaryExpr u, T arg) throws AsterixException;

    R visitUnionExpr(UnionExpr u, T arg) throws AsterixException;

    R visitCreateIndexStatement(CreateIndexStatement cis, T arg) throws AsterixException;

    R visitCreateDataverseStatement(CreateDataverseStatement del, T arg) throws AsterixException;

    R visitIndexDropStatement(IndexDropStatement del, T arg) throws AsterixException;

    R visitNodeGroupDropStatement(NodeGroupDropStatement del, T arg) throws AsterixException;

    R visitDataverseDropStatement(DataverseDropStatement del, T arg) throws AsterixException;

    R visitTypeDropStatement(TypeDropStatement del, T arg) throws AsterixException;

    R visitWriteStatement(WriteStatement ws, T arg) throws AsterixException;

    R visitSetStatement(SetStatement ss, T arg) throws AsterixException;

    R visitDisconnectFeedStatement(DisconnectFeedStatement del, T arg) throws AsterixException;

    R visitConnectFeedStatement(ConnectFeedStatement del, T arg) throws AsterixException;

    R visitCreatePrimaryFeedStatement(CreatePrimaryFeedStatement cpfs, T arg) throws AsterixException;

    R visitCreateSecondaryFeedStatement(CreateSecondaryFeedStatement csfs, T arg) throws AsterixException;

    R visitDropFeedStatement(FeedDropStatement del, T arg) throws AsterixException;
    
    R visitDropFeedPolicyStatement(FeedPolicyDropStatement dfs, T arg) throws AsterixException;

    R visitCreateFeedPolicyStatement(CreateFeedPolicyStatement cfps, T arg) throws AsterixException;

    R visitCallExpr(CallExpr pf, T arg) throws AsterixException;

    R visitDataverseDecl(DataverseDecl dv, T arg) throws AsterixException;

    R visit(CreateFunctionStatement cfs, T arg) throws AsterixException;

    R visitFunctionDropStatement(FunctionDropStatement del, T arg) throws AsterixException;

    R visitCompactStatement(CompactStatement del, T arg) throws AsterixException;

}
