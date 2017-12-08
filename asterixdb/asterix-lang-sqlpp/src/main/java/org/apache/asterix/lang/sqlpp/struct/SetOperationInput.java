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
package org.apache.asterix.lang.sqlpp.struct;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SetOperationInput {

    private SelectBlock selectBlock;
    private SelectExpression subquery;

    public SetOperationInput(SelectBlock selectBlock, SelectExpression subquery) {
        this.selectBlock = selectBlock;
        this.subquery = subquery;
    }

    public SelectBlock getSelectBlock() {
        return selectBlock;
    }

    public SelectExpression getSubquery() {
        return subquery;
    }

    public boolean selectBlock() {
        return selectBlock != null;
    }

    public boolean subquery() {
        return subquery != null;
    }

    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        if (selectBlock != null) {
            return ((ISqlppVisitor<R, T>) visitor).visit(selectBlock, arg);
        } else {
            return ((ISqlppVisitor<R, T>) visitor).visit(subquery, arg);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectBlock, subquery);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SetOperationInput)) {
            return false;
        }
        SetOperationInput target = (SetOperationInput) object;
        return Objects.equals(selectBlock, target.selectBlock) && Objects.equals(subquery, target.subquery);
    }

    @Override
    public String toString() {
        return selectBlock.toString();
    }
}
