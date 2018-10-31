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
package org.apache.asterix.lang.sqlpp.visitor.base;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;

public interface ISqlppVisitor<R, T> extends ILangVisitor<R, T> {

    R visit(FromClause fromClause, T arg) throws CompilationException;

    R visit(FromTerm fromTerm, T arg) throws CompilationException;

    R visit(JoinClause joinClause, T arg) throws CompilationException;

    R visit(NestClause nestClause, T arg) throws CompilationException;

    R visit(Projection projection, T arg) throws CompilationException;

    R visit(SelectBlock selectBlock, T arg) throws CompilationException;

    R visit(SelectClause selectClause, T arg) throws CompilationException;

    R visit(SelectElement selectElement, T arg) throws CompilationException;

    R visit(SelectRegular selectRegular, T arg) throws CompilationException;

    R visit(SelectSetOperation selectSetOperation, T arg) throws CompilationException;

    R visit(SelectExpression selectStatement, T arg) throws CompilationException;

    R visit(UnnestClause unnestClause, T arg) throws CompilationException;

    R visit(HavingClause havingClause, T arg) throws CompilationException;

    R visit(CaseExpression caseExpression, T arg) throws CompilationException;

    R visit(WindowExpression windowExpression, T arg) throws CompilationException;
}
