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

public interface IAqlVisitorWithVoidReturn<T> {

    void visit(Query q, T arg) throws AsterixException;

    void visit(FunctionDecl fd, T arg) throws AsterixException;

    void visit(TypeDecl t, T arg) throws AsterixException;

    void visit(NodegroupDecl ngd, T arg) throws AsterixException;

    void visit(DropStatement stmtDel, T arg) throws AsterixException;

    void visit(LoadFromFileStatement stmtLoad, T arg) throws AsterixException;

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

    void visit(BeginFeedStatement stmtDel, T arg) throws AsterixException;

    void visit(ControlFeedStatement stmtDel, T arg) throws AsterixException;

    void visit(CreateFunctionStatement cfs, T arg) throws AsterixException;

    void visit(FunctionDropStatement fds, T arg) throws AsterixException;

    void visit(CompactStatement fds, T arg) throws AsterixException;
}
