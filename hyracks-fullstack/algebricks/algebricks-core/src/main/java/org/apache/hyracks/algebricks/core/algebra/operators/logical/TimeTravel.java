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

package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.Objects;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

/**
 * The {@code AT SNAPSHOT} / {@code AT TIMESTAMP} specification of a time travelling scan.
 * <p>
 * The value is an ordinary expression reference, exposed through
 * {@link AbstractUnnestNonMapOperator#acceptExpressionTransform} like any other operator expression, so
 * constant folding reduces it in the normal course of optimization. It has to end up a constant: the value
 * selects which snapshot's manifest and data files the scan reads, so it must be known while the plan is still
 * being built, and whoever consumes it (see {@code DatasetRewriter}) rejects anything that did not fold.
 */
public class TimeTravel {

    public enum Type {
        SNAPSHOT_ID("snapshot"),
        SNAPSHOT_TIMESTAMP("timestamp");

        private final String keyword;

        Type(String keyword) {
            this.keyword = keyword;
        }

        public String getKeyword() {
            return keyword;
        }
    }

    private final Mutable<ILogicalExpression> valueExpressionRef;
    private final Type type;

    public TimeTravel(Mutable<ILogicalExpression> valueExpressionRef, Type type) {
        this.valueExpressionRef = valueExpressionRef;
        this.type = type;
    }

    public Mutable<ILogicalExpression> getValueExpressionRef() {
        return valueExpressionRef;
    }

    public ILogicalExpression getValueExpression() {
        return valueExpressionRef.getValue();
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TimeTravel target)) {
            return false;
        }
        return type == target.type && Objects.equals(getValueExpression(), target.getValueExpression());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValueExpression(), type);
    }

    @Override
    public String toString() {
        return type.getKeyword() + " " + getValueExpression();
    }
}
