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
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.visitor.base.AbstractAstVisitor;

public abstract class AbstractAqlAstVisitor<R, T> extends AbstractAstVisitor<R, T> implements IAQLVisitor<R, T> {

    @Override
    public R visit(FLWOGRExpression flwogreExpr, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(UnionExpr u, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(ForClause forClause, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(DistinctClause distinctClause, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(ListSliceExpression expression, T arg) throws CompilationException {
        return null;
    }
}
