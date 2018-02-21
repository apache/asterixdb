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
package org.apache.asterix.lang.common.expression;

import java.util.Objects;

import org.apache.asterix.lang.common.base.Expression;

public class FieldBinding {
    private Expression leftExpr;
    private Expression rightExpr;

    public FieldBinding(Expression leftExpr, Expression rightExpr) {
        super();
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    public Expression getLeftExpr() {
        return leftExpr;
    }

    public void setLeftExpr(Expression leftExpr) {
        this.leftExpr = leftExpr;
    }

    public Expression getRightExpr() {
        return rightExpr;
    }

    public void setRightExpr(Expression rightExpr) {
        this.rightExpr = rightExpr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftExpr, rightExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FieldBinding)) {
            return false;
        }
        FieldBinding target = (FieldBinding) object;
        return Objects.equals(leftExpr, target.leftExpr) && Objects.equals(rightExpr, target.rightExpr);
    }

    @Override
    public String toString() {
        return leftExpr + ": " + rightExpr;
    }

}
