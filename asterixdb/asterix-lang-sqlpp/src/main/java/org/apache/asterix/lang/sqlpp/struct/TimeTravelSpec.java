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

import org.apache.asterix.lang.common.base.Expression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * The unresolved form of an {@code AT SNAPSHOT} / {@code AT TIMESTAMP} specification.
 * <p>
 * The value is carried as an {@link Expression} so that the language rewrites (function call resolution,
 * operator-to-function-call conversion, UDF inlining, ...) can process it like any other expression. It must
 * reduce to a compile-time constant: expression-to-plan translation folds it into the {@link TimeTravel} that
 * the plan carries, because the snapshot selects which Iceberg manifest/data files the scan reads and therefore
 * has to be known while the plan is still being built.
 */
public final class TimeTravelSpec {

    private final TimeTravel.Type type;

    private Expression valueExpr;

    private SourceLocation sourceLoc;

    public TimeTravelSpec(Expression valueExpr, TimeTravel.Type type) {
        this.valueExpr = Objects.requireNonNull(valueExpr, "valueExpr");
        this.type = Objects.requireNonNull(type, "type");
    }

    public Expression getValueExpression() {
        return valueExpr;
    }

    public void setValueExpression(Expression valueExpr) {
        this.valueExpr = Objects.requireNonNull(valueExpr, "valueExpr");
    }

    public TimeTravel.Type getType() {
        return type;
    }

    public SourceLocation getSourceLocation() {
        return sourceLoc;
    }

    public void setSourceLocation(SourceLocation sourceLoc) {
        this.sourceLoc = sourceLoc;
    }

    /**
     * @return the {@code SNAPSHOT} / {@code TIMESTAMP} keyword this specification was written with.
     */
    public String getKeyword() {
        return type.getKeyword();
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueExpr, type);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TimeTravelSpec target)) {
            return false;
        }
        return type == target.type && Objects.equals(valueExpr, target.valueExpr);
    }

    @Override
    public String toString() {
        return type.getKeyword() + " " + valueExpr;
    }
}
