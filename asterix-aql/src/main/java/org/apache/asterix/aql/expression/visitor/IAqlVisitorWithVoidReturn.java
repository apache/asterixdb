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
import org.apache.asterix.aql.expression.CreateFeedStatement;
import org.apache.asterix.aql.expression.CreateFunctionStatement;
import org.apache.asterix.aql.expression.CreateIndexStatement;
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
import org.apache.asterix.aql.expression.RunStatement;
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

public interface IAqlVisitorWithVoidReturn<T> {

    void visit(Query q, T arg) throws AsterixException;

    void visit(FunctionDecl fd, T arg) throws AsterixException;

    void visit(TypeDecl t, T arg) throws AsterixException;

    void visit(NodegroupDecl ngd, T arg) throws AsterixException;

    void visit(DropStatement stmtDel, T arg) throws AsterixException;

    void visit(LoadStatement stmtLoad, T arg) throws AsterixException;

    void visit(InsertStatement stmtInsert, T arg) throws AsterixException;

    void visit(DeleteStatement stmtDelete, T arg) throws AsterixException;

    void visit(UpdateStatement stmtUpdate, T arg) throws AsterixException;

    void visit(UpdateClause updateClause, T arg) throws AsterixException;

    void visit(DatasetDecl dd, T arg) throws AsterixException;

    void visit(LiteralExpr l, T arg) throws AsterixException;

    void visit(VariableExpr v, T arg) throws AsterixException;

    void visit(ListConstructor lc, T arg) throws AsterixException;

    void visit(RecordConstructor rc, T arg) throws AsterixException;

    void visit(CallExpr pf, T arg) throws AsterixException;

    void visit(OperatorExpr ifbo, T arg) throws AsterixException;

    void visit(FieldAccessor fa, T arg) throws AsterixException;

    void visit(IndexAccessor fa, T arg) throws AsterixException;

    void visit(IfExpr ifexpr, T arg) throws AsterixException;

    void visit(FLWOGRExpression flwor, T arg) throws AsterixException;

    void visit(QuantifiedExpression qe, T arg) throws AsterixException;

    void visit(ForClause fc, T arg) throws AsterixException;

    void visit(LetClause lc, T arg) throws AsterixException;

    void visit(WhereClause wc, T arg) throws AsterixException;

    void visit(OrderbyClause oc, T arg) throws AsterixException;

    void visit(GroupbyClause gc, T arg) throws AsterixException;

    void visit(LimitClause lc, T arg) throws AsterixException;

    void visit(DistinctClause dc, T arg) throws AsterixException;

    void visit(UnaryExpr u, T arg) throws AsterixException;

    void visit(UnionExpr u, T arg) throws AsterixException;

    void visit(TypeReferenceExpression t, T arg) throws AsterixException;

    void visit(RecordTypeDefinition r, T arg) throws AsterixException;

    void visit(OrderedListTypeDefinition x, T arg) throws AsterixException;

    void visit(UnorderedListTypeDefinition x, T arg) throws AsterixException;

    void visit(DataverseDecl dv, T arg) throws AsterixException;

    void visit(SetStatement ss, T arg) throws AsterixException;

    void visit(WriteStatement ws, T arg) throws AsterixException;

    void visit(CreateIndexStatement cis, T arg) throws AsterixException;

    void visit(CreateDataverseStatement cdvStmt, T arg) throws AsterixException;

    void visit(IndexDropStatement stmtDel, T arg) throws AsterixException;

    void visit(NodeGroupDropStatement stmtDel, T arg) throws AsterixException;

    void visit(DataverseDropStatement stmtDel, T arg) throws AsterixException;

    void visit(TypeDropStatement stmtDel, T arg) throws AsterixException;

    void visit(DisconnectFeedStatement stmtDel, T arg) throws AsterixException;

    void visit(ConnectFeedStatement stmtDel, T arg) throws AsterixException;

    void visit(CreateFeedStatement stmt, T arg) throws AsterixException;

    void visit(CreateFeedPolicyStatement stmt, T arg) throws AsterixException;

    void visit(FeedDropStatement stmt, T arg) throws AsterixException;

    void visit(FeedPolicyDropStatement stmt, T arg) throws AsterixException;

    void visit(CreateFunctionStatement cfs, T arg) throws AsterixException;

    void visit(FunctionDropStatement fds, T arg) throws AsterixException;

    void visit(CompactStatement fds, T arg) throws AsterixException;

    void visit(RunStatement stmt, T arg) throws AsterixException;
}
