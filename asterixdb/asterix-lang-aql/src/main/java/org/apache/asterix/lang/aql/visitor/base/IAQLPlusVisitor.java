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
package org.apache.asterix.lang.aql.visitor.base;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.clause.JoinClause;
import org.apache.asterix.lang.aql.clause.MetaVariableClause;
import org.apache.asterix.lang.aql.expression.MetaVariableExpr;

public interface IAQLPlusVisitor<R, T> extends IAQLVisitor<R, T> {

    R visitJoinClause(JoinClause c, T arg) throws CompilationException;

    R visitMetaVariableClause(MetaVariableClause c, T arg) throws CompilationException;

    R visitMetaVariableExpr(MetaVariableExpr v, T arg) throws CompilationException;
}
