/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression.visitor;

import edu.uci.ics.asterix.aql.expression.BeginFeedStatement;
import edu.uci.ics.asterix.aql.expression.CallExpr;
import edu.uci.ics.asterix.aql.expression.CompactStatement;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateDataverseStatement;
import edu.uci.ics.asterix.aql.expression.CreateFunctionStatement;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.DistinctClause;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.FLWOGRExpression;
import edu.uci.ics.asterix.aql.expression.FieldAccessor;
import edu.uci.ics.asterix.aql.expression.ForClause;
import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.GroupbyClause;
import edu.uci.ics.asterix.aql.expression.IfExpr;
import edu.uci.ics.asterix.aql.expression.IndexAccessor;
import edu.uci.ics.asterix.aql.expression.IndexDropStatement;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.LetClause;
import edu.uci.ics.asterix.aql.expression.LimitClause;
import edu.uci.ics.asterix.aql.expression.ListConstructor;
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.LoadFromFileStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.OperatorExpr;
import edu.uci.ics.asterix.aql.expression.OrderbyClause;
import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.QuantifiedExpression;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnaryExpr;
import edu.uci.ics.asterix.aql.expression.UnionExpr;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.UpdateClause;
import edu.uci.ics.asterix.aql.expression.UpdateStatement;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IAqlExpressionVisitor<R, T> {

    R visitQuery(Query q, T arg) throws AsterixException;

    R visitFunctionDecl(FunctionDecl fd, T arg) throws AsterixException;

    R visitTypeDecl(TypeDecl td, T arg) throws AsterixException;

    R visitNodegroupDecl(NodegroupDecl ngd, T arg) throws AsterixException;

    R visitDatasetDecl(DatasetDecl dd, T arg) throws AsterixException;

    R visitLoadFromFileStatement(LoadFromFileStatement stmtLoad, T arg) throws AsterixException;

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

    R visitBeginFeedStatement(BeginFeedStatement bf, T arg) throws AsterixException;

    R visitControlFeedStatement(ControlFeedStatement del, T arg) throws AsterixException;

    R visitCallExpr(CallExpr pf, T arg) throws AsterixException;

    R visitDataverseDecl(DataverseDecl dv, T arg) throws AsterixException;

    R visit(CreateFunctionStatement cfs, T arg) throws AsterixException;

    R visitFunctionDropStatement(FunctionDropStatement del, T arg) throws AsterixException;

    R visitCompactStatement(CompactStatement del, T arg) throws AsterixException;

}
